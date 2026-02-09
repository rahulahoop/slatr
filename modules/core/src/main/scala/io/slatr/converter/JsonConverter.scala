package io.slatr.converter

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.syntax._
import io.slatr.model.{Chunk, OutputConfig, Schema}
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
      val jsonArray = elements.map(elementToJson)
      val json = Json.arr(jsonArray: _*)
      
      // Write to file
      val jsonString = if (outputConfig.pretty) {
        json.spaces2
      } else {
        json.noSpaces
      }
      
      writer.write(jsonString)
      logger.info(s"Successfully wrote JSON to ${outputFile.getAbsolutePath}")
    }.get
    
    outputFile
  }
  
  /**
   * Convert an element map to Circe JSON
   */
  private def elementToJson(element: Map[String, Any]): Json = {
    val fields = element.map { case (key, value) =>
      key -> valueToJson(value)
    }
    Json.obj(fields.toSeq: _*)
  }
  
  /**
   * Convert a value to Circe JSON
   */
  private def valueToJson(value: Any): Json = value match {
    case null => Json.Null
    case s: String => Json.fromString(s)
    case i: Int => Json.fromInt(i)
    case l: Long => Json.fromLong(l)
    case d: Double => Json.fromDoubleOrNull(d)
    case b: Boolean => Json.fromBoolean(b)
    case list: List[_] => Json.arr(list.map(valueToJson): _*)
    case map: Map[_, _] => elementToJson(map.asInstanceOf[Map[String, Any]])
    case other => Json.fromString(other.toString)
  }
}

object JsonConverter {
  def apply(xmlParser: XmlStreamParser): JsonConverter = new JsonConverter(xmlParser)
}
