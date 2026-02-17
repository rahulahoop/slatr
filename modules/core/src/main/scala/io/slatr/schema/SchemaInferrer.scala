package io.slatr.schema

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model._
import io.slatr.parser.XmlStreamParser

import java.io.File
import scala.collection.mutable
import scala.util.Try

/** Infers schema from XML files using multiple strategies */
class SchemaInferrer(
  xsdResolver: XsdResolver,
  xmlParser: XmlStreamParser
) extends LazyLogging {
  
  /**
   * Infer schema from XML file according to configuration
   */
  def infer(file: File, config: SchemaConfig): Try[Schema] = Try {
    logger.info(s"Inferring schema from ${file.getName} using mode: ${config.mode}")
    
    val rootElement = xmlParser.getRootElementName(file)
      .getOrElse(throw new Exception("Could not determine root element"))
    
    config.mode match {
      case SchemaMode.Auto =>
        inferFromSampling(file, config.sampling, rootElement)
        
      case SchemaMode.Xsd =>
        inferFromXsd(file, rootElement)
          .getOrElse(throw new Exception("XSD resolution failed and mode is XSD-only"))
        
      case SchemaMode.Manual =>
        inferFromManualOverrides(config.overrides, rootElement)
        
      case SchemaMode.Hybrid =>
        inferHybrid(file, config, rootElement)
    }
  }
  
  /**
   * Hybrid approach: XSD first, fill gaps with sampling, apply manual overrides
   */
  private def inferHybrid(file: File, config: SchemaConfig, rootElement: String): Schema = {
    // 1. Try XSD-based inference
    val xsdSchema = inferFromXsd(file, rootElement)
    
    // 2. Sample-based inference
    val sampledSchema = inferFromSampling(file, config.sampling, rootElement)
    
    // 3. Merge: XSD takes precedence
    val merged = xsdSchema match {
      case Some(xsd) =>
        logger.debug("Merging XSD schema with sampled schema")
        xsd.merge(sampledSchema)
      case None =>
        logger.debug("No XSD available, using sampled schema")
        sampledSchema
    }
    
    // 4. Apply manual overrides
    applyOverrides(merged, config.overrides)
  }
  
  /**
   * Infer schema from XSD
   */
  private def inferFromXsd(file: File, rootElement: String): Option[Schema] = {
    xsdResolver.resolveFromXml(file, xmlParser).map { xsdSchema =>
      logger.info(s"Converting XSD schema with ${xsdSchema.elements.size} elements")
      
      val fields = xsdSchema.elements.map { case (name, xsdElem) =>
        name -> Field(
          name = name,
          dataType = xsdElem.dataType,
          nullable = !xsdElem.isRequired || xsdElem.isNillable,
          isArray = xsdElem.isArray
        )
      }
      
      Schema(rootElement, fields)
    }
  }
  
  /**
   * Infer schema by sampling XML elements
   */
  private def inferFromSampling(
    file: File,
    samplingConfig: SamplingConfig,
    rootElement: String
  ): Schema = {
    logger.info(s"Sampling up to ${samplingConfig.size} elements from XML")
    
    val accumulatedFields = mutable.Map[String, Field]()
    
    xmlParser.parseNamed(file).foreach { iterator =>
      iterator.take(samplingConfig.size).foreach { case (elemName, contents) =>
        // Each (elemName, contents) is a depth-2 child of the root.
        // Build a field for this element — a StructType if it has sub-elements.
        val field = inferField(elemName, contents)
        accumulatedFields.get(elemName) match {
          case Some(existing) =>
            accumulatedFields(elemName) = mergeFields(existing, field)
          case None =>
            accumulatedFields(elemName) = field
        }
      }
    }
    
    logger.info(s"Inferred ${accumulatedFields.size} fields from sampling")
    Schema(rootElement, accumulatedFields.toMap)
  }
  
  /**
   * Infer a Field from a parsed value (recursive).
   * Builds StructType for maps, ArrayType for lists, and leaf types for strings.
   */
  private def inferField(name: String, value: Any): Field = {
    value match {
      case list: List[_] =>
        if (list.isEmpty) {
          Field(name, DataType.StringType, nullable = true, isArray = true)
        } else {
          list.head match {
            case _: Map[_, _] =>
              // List of objects — infer type from all items, merge across them
              val elemType = list.foldLeft(Option.empty[DataType]) { (acc, item) =>
                val itemType = inferMapType(item.asInstanceOf[Map[String, Any]])
                acc match {
                  case None => Some(itemType)
                  case Some(prev) => Some(mergeDataTypes(prev, itemType))
                }
              }.getOrElse(DataType.StringType)
              Field(name, elemType, nullable = true, isArray = true)
            case _ =>
              // List of primitives
              val elemType = inferType(list.head.toString)
              Field(name, elemType, nullable = true, isArray = true)
          }
        }
        
      case map: Map[_, _] =>
        val dataType = inferMapType(map.asInstanceOf[Map[String, Any]])
        Field(name, dataType, nullable = true, isArray = false)
        
      case str: String =>
        Field(name, inferType(str), nullable = true, isArray = false)
        
      case _ =>
        Field(name, DataType.StringType, nullable = true, isArray = false)
    }
  }
  
  /**
   * Infer the DataType for a parsed map.
   * If the map is text-only (just #text and optional @attrs), returns a leaf type.
   * Otherwise returns a StructType.
   */
  private def inferMapType(m: Map[String, Any]): DataType = {
    val nonAttrKeys = m.keys.filterNot(_.startsWith("@")).toSet
    if (nonAttrKeys == Set("#text")) {
      inferType(m("#text").toString)
    } else if (nonAttrKeys.isEmpty) {
      // Attributes only, no text or children
      DataType.StringType
    } else {
      DataType.StructType(inferStructFields(m))
    }
  }
  
  /**
   * Merge two DataTypes, reconciling mismatches.
   */
  private def mergeDataTypes(a: DataType, b: DataType): DataType = {
    (a, b) match {
      case (at, bt) if at == bt => at
      case (DataType.StructType(af), DataType.StructType(bf)) =>
        DataType.StructType(mergeFieldMaps(af, bf))
      case _ => DataType.StringType
    }
  }
  
  /**
   * Infer fields for a struct (map of name -> value).
   */
  private def inferStructFields(element: Map[String, Any]): Map[String, Field] = {
    element.map { case (name, value) =>
      name -> inferField(name, value)
    }
  }
  
  /**
   * Merge two fields with the same name, reconciling types.
   */
  private def mergeFields(a: Field, b: Field): Field = {
    val mergedType = (a.dataType, b.dataType) match {
      case (at, bt) if at == bt => at
      case (DataType.StructType(af), DataType.StructType(bf)) =>
        DataType.StructType(mergeFieldMaps(af, bf))
      case _ => DataType.StringType // type conflict — fall back to string
    }
    Field(a.name, mergedType, nullable = a.nullable || b.nullable, isArray = a.isArray || b.isArray)
  }
  
  /**
   * Merge two field maps, reconciling overlapping fields.
   */
  private def mergeFieldMaps(a: Map[String, Field], b: Map[String, Field]): Map[String, Field] = {
    val allKeys = a.keySet ++ b.keySet
    allKeys.map { key =>
      (a.get(key), b.get(key)) match {
        case (Some(af), Some(bf)) => key -> mergeFields(af, bf)
        case (Some(af), None)     => key -> af.copy(nullable = true)
        case (None, Some(bf))     => key -> bf.copy(nullable = true)
        case _                    => key -> Field(key, DataType.StringType, nullable = true, isArray = false) // unreachable
      }
    }.toMap
  }
  
  /**
   * Infer data type from string value
   */
  private def inferType(value: String): DataType = {
    if (value == "true" || value == "false") DataType.BooleanType
    else if (value.matches("-?\\d+")) {
      if (value.length <= 10) DataType.IntType else DataType.LongType
    }
    else if (value.matches("-?\\d+\\.\\d+")) DataType.DoubleType
    else if (value.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*")) DataType.TimestampType
    else if (value.matches("\\d{4}-\\d{2}-\\d{2}")) DataType.DateType
    else DataType.StringType
  }
  
  /**
   * Create schema from manual overrides only
   */
  private def inferFromManualOverrides(overrides: SchemaOverrides, rootElement: String): Schema = {
    logger.info("Using manual schema overrides")
    
    val fields = overrides.typeHints.map { case (path, typeHint) =>
      val dataType = DataType.fromXsdType(typeHint)
      val isArray = overrides.forceArrays.contains(path)
      
      path -> Field(
        name = path,
        dataType = dataType,
        nullable = true,
        isArray = isArray
      )
    }
    
    Schema(rootElement, fields)
  }
  
  /**
   * Apply manual overrides to an existing schema
   */
  private def applyOverrides(schema: Schema, overrides: SchemaOverrides): Schema = {
    if (overrides.forceArrays.isEmpty && overrides.typeHints.isEmpty) schema
    else {
      logger.info(s"Applying manual overrides: ${overrides.forceArrays.size} force-arrays, " +
        s"${overrides.typeHints.size} type hints")

      val afterArrays = overrides.forceArrays.foldLeft(schema) { (acc, path) =>
        acc.fields.get(path) match {
          case Some(field) => acc.withField(path, field.copy(isArray = true))
          case None        => acc
        }
      }

      overrides.typeHints.foldLeft(afterArrays) { case (acc, (path, typeHint)) =>
        val dataType = DataType.fromXsdType(typeHint)
        acc.fields.get(path) match {
          case Some(field) => acc.withField(path, field.copy(dataType = dataType))
          case None        => acc.withField(path, Field(path, dataType, nullable = true, isArray = false))
        }
      }
    }
  }
}

object SchemaInferrer {
  def apply(xsdResolver: XsdResolver, xmlParser: XmlStreamParser): SchemaInferrer = {
    new SchemaInferrer(xsdResolver, xmlParser)
  }
}
