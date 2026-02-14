package io.slatr.cli.commands

import com.monovore.decline._
import io.slatr.cli.config.ConfigLoader
import io.slatr.converter.{JsonConverter, JsonLinesConverter, ParquetConverter}
import io.slatr.model._
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}

import java.io.File
import cats.implicits._

object ConvertCommand {
  
  val inputOpt: Opts[File] = Opts.argument[String]("input")
    .mapValidated(s => new File(s).validNel)
    .validate("Input file must exist")(_.exists())
  
  val outputOpt: Opts[File] = Opts.option[String]("output", short = "o", help = "Output file path")
    .mapValidated(s => new File(s).validNel)
  
  val formatOpt: Opts[OutputFormat] = Opts.option[String]("format", short = "f", help = "Output format (json, jsonl, parquet)")
    .withDefault("json")
    .map(OutputFormat.fromString)
  
  val configOpt: Opts[Option[File]] = Opts.option[String]("config", short = "c", help = "Config file path")
    .mapValidated(s => new File(s).validNel)
    .orNone
  
  val prettyOpt: Opts[Boolean] = Opts.flag("pretty", help = "Pretty-print JSON output")
    .orFalse
  
  val validateOpt: Opts[Boolean] = Opts.flag("validate", help = "Validate XML against XSD")
    .orFalse
  
  val dryRunOpt: Opts[Boolean] = Opts.flag("dry-run", help = "Infer schema only, don't convert")
    .orFalse
  
  val command: Command[Unit] = Command(
    name = "convert",
    header = "Convert XML file to modern formats"
  ) {
    (inputOpt, outputOpt, formatOpt, configOpt, prettyOpt, validateOpt, dryRunOpt).mapN {
      (input, output, format, configFile, pretty, validate, dryRun) =>
        
        println(s"🔄 Converting ${input.getName} to ${format}...")
        
        // Load or create config
        val config = configFile match {
          case Some(cf) =>
            println(s"Loading config from ${cf.getName}")
            ConfigLoader.loadFromFile(cf).getOrElse {
              println(s"Warning: Failed to load config from ${cf.getName}, using defaults")
              ConfigLoader.defaultConfig(input.getAbsolutePath, output.getAbsolutePath)
            }
          case None =>
            ConfigLoader.defaultConfig(input.getAbsolutePath, output.getAbsolutePath)
        }
        
        // Override config with CLI options
        val finalConfig = config.copy(
          output = config.output.copy(
            format = format,
            path = output.getAbsolutePath,
            pretty = pretty
          ),
          schema = config.schema.copy(
            xsd = config.schema.xsd.copy(validate = validate)
          )
        )
        
        // Initialize components
        val xmlParser = XmlStreamParser()
        val xsdResolver = XsdResolver(finalConfig.schema.xsd)
        val inferrer = SchemaInferrer(xsdResolver, xmlParser)
        
        // Infer schema
        println("Inferring schema...")
        val schemaResult = inferrer.infer(input, finalConfig.schema)
        
        schemaResult match {
          case scala.util.Success(schema) =>
            println(s"✓ Schema inferred: ${schema.fields.size} fields")
            
            if (dryRun) {
              println("\n📋 Schema preview:")
              schema.fields.foreach { case (name, field) =>
                val arrayMarker = if (field.isArray) "[]" else ""
                println(s"  $name: ${field.dataType}$arrayMarker")
              }
            } else {
              // Perform conversion
              println("Converting...")
              
              val converter = format match {
                case OutputFormat.Json => JsonConverter(xmlParser)
                case OutputFormat.JsonLines => JsonLinesConverter(xmlParser)
                case OutputFormat.Parquet => ParquetConverter(xmlParser)
                case _ => throw new Exception(s"Unsupported format: $format")
              }
              
              converter.convert(input, schema, finalConfig.output, None) match {
                case scala.util.Success(outputFile) =>
                  println(s"✅ Successfully converted to ${outputFile.getAbsolutePath}")
                  
                case scala.util.Failure(ex) =>
                  println(s"❌ Conversion failed: ${ex.getMessage}")
                  sys.exit(1)
              }
            }
            
          case scala.util.Failure(ex) =>
            println(s"❌ Schema inference failed: ${ex.getMessage}")
            sys.exit(1)
        }
    }
  }
}
