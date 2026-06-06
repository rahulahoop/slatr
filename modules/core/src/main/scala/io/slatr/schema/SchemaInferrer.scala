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
      iterator.take(samplingConfig.size).foreach { case (elemName, element) =>
        // Each (elemName, element) is a depth-2 child of the root.
        // Build a field for this element — a StructType if it has sub-elements.
        val field = Field(elemName, dataTypeOf(element), nullable = true, isArray = false)
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
   * Infer a Field for a named child that occurs `elements` times under its parent.
   * A single occurrence is a scalar/struct; multiple occurrences are an array.
   */
  private def inferElementField(name: String, elements: List[XmlElement]): Field =
    elements match {
      case Nil =>
        Field(name, DataType.StringType, nullable = true, isArray = true)
      case single :: Nil =>
        Field(name, dataTypeOf(single), nullable = true, isArray = false)
      case many =>
        val elemType = many.map(dataTypeOf).reduce(mergeDataTypes)
        Field(name, elemType, nullable = true, isArray = true)
    }

  /**
   * Infer the DataType of an element: a leaf type from its text when it has no children,
   * otherwise a StructType over its attributes (as `@name` fields) and child elements.
   */
  private def dataTypeOf(element: XmlElement): DataType =
    if (element.children.isEmpty) {
      element.text.map(inferType).getOrElse(DataType.StringType)
    } else {
      val attrFields = element.attributes.map { case (k, _) =>
        s"@$k" -> Field(s"@$k", DataType.StringType, nullable = true, isArray = false)
      }
      val childFields = element.children.map { case (name, els) => name -> inferElementField(name, els) }
      DataType.StructType(attrFields ++ childFields)
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
