package io.slatr.cli.config

import io.slatr.model._

import java.io.File
import scala.io.Source
import scala.util.{Try, Using}

/** Loads configuration from JSON files. */
object ConfigLoader {

  /** Load configuration from a JSON file. */
  def loadFromFile(file: File): Try[SlatrConfig] = Try {
    Using(Source.fromFile(file)) { source =>
      parseJson(source.mkString)
    }.get
  }

  /** Parse a JSON string into a [[SlatrConfig]]. Throws on malformed JSON or invalid shape. */
  def parseJson(jsonContent: String): SlatrConfig =
    upickle.default.read[SlatrConfig](jsonContent)

  /** Create a default configuration. */
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
