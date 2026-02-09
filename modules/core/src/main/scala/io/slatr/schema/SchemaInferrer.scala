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
    
    val fieldTypes = mutable.Map[String, DataType]()
    val fieldArrayness = mutable.Map[String, Boolean]()
    val fieldCounts = mutable.Map[String, Int]()
    
    xmlParser.parse(file, None).foreach { iterator =>
      iterator.take(samplingConfig.size).foreach { element =>
        analyzeElement(element, fieldTypes, fieldArrayness, fieldCounts)
      }
    }
    
    logger.info(s"Inferred ${fieldTypes.size} fields from sampling")
    
    val fields = fieldTypes.map { case (name, dataType) =>
      name -> Field(
        name = name,
        dataType = dataType,
        nullable = true, // Default to nullable from sampling
        isArray = fieldArrayness.getOrElse(name, false)
      )
    }.toMap
    
    Schema(rootElement, fields)
  }
  
  /**
   * Analyze a single element and update field type information
   */
  private def analyzeElement(
    element: Map[String, Any],
    fieldTypes: mutable.Map[String, DataType],
    fieldArrayness: mutable.Map[String, Boolean],
    fieldCounts: mutable.Map[String, Int],
    prefix: String = ""
  ): Unit = {
    element.foreach { case (name, value) =>
      val fullName = if (prefix.isEmpty) name else s"$prefix.$name"
      fieldCounts(fullName) = fieldCounts.getOrElse(fullName, 0) + 1
      
      value match {
        case list: List[_] =>
          // It's a list - mark as array
          fieldArrayness(fullName) = true
          if (list.nonEmpty) {
            list.head match {
              case map: Map[_, _] =>
                // List of objects
                val structFields = mutable.Map[String, DataType]()
                list.foreach { item =>
                  analyzeElement(
                    item.asInstanceOf[Map[String, Any]],
                    fieldTypes,
                    fieldArrayness,
                    fieldCounts,
                    fullName
                  )
                }
              case _ =>
                // List of primitives
                val inferredType = inferType(list.head.toString)
                fieldTypes(fullName) = DataType.ArrayType(inferredType)
            }
          }
          
        case map: Map[_, _] =>
          // Nested object
          analyzeElement(
            map.asInstanceOf[Map[String, Any]],
            fieldTypes,
            fieldArrayness,
            fieldCounts,
            fullName
          )
          
        case str: String =>
          val inferredType = inferType(str)
          fieldTypes.get(fullName) match {
            case Some(existing) if existing != inferredType =>
              // Type conflict - default to string
              fieldTypes(fullName) = DataType.StringType
            case None =>
              fieldTypes(fullName) = inferredType
            case _ => // Keep existing
          }
      }
    }
  }
  
  /**
   * Infer data type from string value
   */
  private def inferType(value: String): DataType = {
    // Try boolean
    if (value == "true" || value == "false") return DataType.BooleanType
    
    // Try integer
    if (value.matches("-?\\d+")) {
      return if (value.length <= 10) DataType.IntType else DataType.LongType
    }
    
    // Try double
    if (value.matches("-?\\d+\\.\\d+")) return DataType.DoubleType
    
    // Try timestamp (ISO 8601)
    if (value.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*")) {
      return DataType.TimestampType
    }
    
    // Try date
    if (value.matches("\\d{4}-\\d{2}-\\d{2}")) return DataType.DateType
    
    // Default to string
    DataType.StringType
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
    if (overrides.forceArrays.isEmpty && overrides.typeHints.isEmpty) {
      return schema
    }
    
    logger.info(s"Applying manual overrides: ${overrides.forceArrays.size} force-arrays, " +
      s"${overrides.typeHints.size} type hints")
    
    var updated = schema
    
    // Apply force-arrays
    overrides.forceArrays.foreach { path =>
      updated.fields.get(path).foreach { field =>
        updated = updated.withField(path, field.copy(isArray = true))
      }
    }
    
    // Apply type hints
    overrides.typeHints.foreach { case (path, typeHint) =>
      val dataType = DataType.fromXsdType(typeHint)
      updated.fields.get(path) match {
        case Some(field) =>
          updated = updated.withField(path, field.copy(dataType = dataType))
        case None =>
          // Create new field
          updated = updated.withField(
            path,
            Field(path, dataType, nullable = true, isArray = false)
          )
      }
    }
    
    updated
  }
}

object SchemaInferrer {
  def apply(xsdResolver: XsdResolver, xmlParser: XmlStreamParser): SchemaInferrer = {
    new SchemaInferrer(xsdResolver, xmlParser)
  }
}
