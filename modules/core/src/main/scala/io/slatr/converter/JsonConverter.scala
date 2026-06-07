package io.slatr.converter

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{Chunk, OutputConfig, Schema, XmlElement}
import io.slatr.parser.XmlStreamParser

import java.io.{File, FileWriter}
import scala.util.{Try, Using}

/** Converts XML to JSON format */
class JsonConverter(xmlParser: XmlStreamParser) extends Converter with LazyLogging {
  
  override def fileExtension: String = "json"
  
  override def convert(
    xmlFile: File,
    schema: Schema,
    outputConfig: OutputConfig,
    chunk: Option[Chunk]
  ): Try[File] = Try {
    logger.info(s"Converting ${xmlFile.getName} to JSON")
    
    val outputFile = new File(outputConfig.path)
    
    Using(new FileWriter(outputFile)) { writer =>
      val elements = xmlParser.parse(xmlFile, chunk)
        .getOrElse(throw new Exception("Failed to parse XML"))
        .toList
      
      logger.info(s"Parsed ${elements.size} elements")
      
      // Convert to JSON array
      val json = ujson.Arr.from(elements.map(elementToJson))

      // Write to file
      val jsonString = if (outputConfig.pretty) ujson.write(json, indent = 2) else ujson.write(json)

      writer.write(jsonString)
      logger.info(s"Successfully wrote JSON to ${outputFile.getAbsolutePath}")
    }.get

    outputFile
  }

  /**
   * Convert an [[XmlElement]] to JSON: attributes as `@name`, leaf text as `#text`,
   * and child elements as arrays keyed by tag name.
   */
  private def elementToJson(element: XmlElement): ujson.Value = {
    val attrFields  = element.attributes.map { case (k, v) => s"@$k" -> (ujson.Str(v): ujson.Value) }
    val textField   = element.text.map(t => "#text" -> (ujson.Str(t): ujson.Value)).toMap
    val childFields = element.children.map { case (name, els) =>
      name -> (ujson.Arr.from(els.map(elementToJson)): ujson.Value)
    }
    ujson.Obj.from(attrFields ++ textField ++ childFields)
  }
}

object JsonConverter {
  def apply(xmlParser: XmlStreamParser): JsonConverter = new JsonConverter(xmlParser)
}
