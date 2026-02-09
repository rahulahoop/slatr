package io.slatr.cli.config

import io.circe.generic.auto._
import io.circe.yaml.parser
import io.slatr.model._

import java.io.File
import scala.io.Source
import scala.util.{Try, Using}

/** Loads configuration from YAML files */
object ConfigLoader {
  
  /**
   * Load configuration from YAML file
   */
  def loadFromFile(file: File): Try[SlatrConfig] = Try {
    Using(Source.fromFile(file)) { source =>
      val yamlContent = source.mkString
      parseYaml(yamlContent)
    }.get
  }
  
  /**
   * Parse YAML string to config
   */
  def parseYaml(yamlContent: String): SlatrConfig = {
    parser.parse(yamlContent) match {
      case Right(json) =>
        json.as[SlatrConfig] match {
          case Right(config) => config
          case Left(error) =>
            throw new Exception(s"Failed to decode config: ${error.getMessage}")
        }
      case Left(error) =>
        throw new Exception(s"Failed to parse YAML: ${error.getMessage}")
    }
  }
  
  /**
   * Create default configuration
   */
  def defaultConfig(inputPath: String, outputPath: String): SlatrConfig = {
    SlatrConfig(
      input = InputConfig(path = inputPath),
      schema = SchemaConfig(),
      chunking = ChunkingConfig(enabled = false),
      output = OutputConfig(path = outputPath),
      logging = LoggingConfig()
    )
  }
}
