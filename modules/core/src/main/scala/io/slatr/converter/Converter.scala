package io.slatr.converter

import io.slatr.model.{Chunk, OutputConfig, Schema}

import java.io.File
import scala.util.Try

/** Base trait for format converters */
trait Converter {
  
  /**
   * Convert XML file to target format
   * @param xmlFile The source XML file
   * @param schema The inferred schema
   * @param outputConfig Output configuration
   * @param chunk Optional chunk specification
   * @return Success or failure
   */
  def convert(
    xmlFile: File,
    schema: Schema,
    outputConfig: OutputConfig,
    chunk: Option[Chunk] = None
  ): Try[File]
  
  /**
   * Get the output file extension for this converter
   */
  def fileExtension: String
}
