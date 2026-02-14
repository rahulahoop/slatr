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
  
  import scala.io.StdIn
  import scala.util.{Try, Success, Failure}
  
  // Interactive input method for XML file
  def interactiveInputFile(): File = {
    println("\n🔍 XML to JSON Converter")
    println("-------------------------")
    
    // Repeatedly prompt until a valid input file is provided
    while (true) {
      print("Enter the path to your XML file: ")
      val inputPath = StdIn.readLine().trim
      
      if (inputPath.isEmpty) {
        println("❌ Input path cannot be empty. Please try again.")
      } else {
        val inputFile = new File(inputPath)
        
        if (!inputFile.exists()) {
          println(s"❌ File not found: $inputPath. Please check the file path.")
        } else if (!inputFile.isFile) {
          println(s"❌ Not a valid file: $inputPath. Please provide a file path.")
        } else if (!inputPath.toLowerCase.endsWith(".xml")) {
          println("❌ Please provide a valid XML file (with .xml extension).")
        } else {
          return inputFile
        }
      }
    }
    
    // This line will never be reached due to the while loop, 
    // but Scala requires a return value
    throw new IllegalStateException("Unreachable code")
  }
  
  // Interactive output file selection
  def interactiveOutputFile(inputFileName: String): File = {
    // Default output file name based on input
    val defaultOutputName = inputFileName.replaceFirst("[.][^.]+$", ".json")
    
    print(s"\nOutput file name (default: $defaultOutputName): ")
    val outputName = StdIn.readLine().trim
    
    new File(if (outputName.isEmpty) defaultOutputName else outputName)
  }
  
  // Interactive format selection
  def interactiveFormatSelection(): OutputFormat = {
    println("\nSelect output format:")
    println("1. JSON (default)")
    println("2. JSON Lines")
    println("3. Parquet")
    
    while (true) {
      print("Enter your choice (1-3): ")
      StdIn.readLine().trim match {
        case "1" | "" => return OutputFormat.Json
        case "2" => return OutputFormat.JsonLines
        case "3" => return OutputFormat.Parquet
        case _ => println("❌ Invalid selection. Please choose 1, 2, or 3.")
      }
    }
    
    // This line will never be reached
    throw new IllegalStateException("Unreachable code")
  }
  
  // Interactive configuration
  def interactiveConfiguration(): (Boolean, Boolean, Boolean) = {
    println("\nAdditional Conversion Options:")
    
    print("Enable pretty-print JSON? (y/N): ")
    val pretty = StdIn.readLine().trim.toLowerCase == "y"
    
    print("Validate XML against XSD? (y/N): ")
    val validate = StdIn.readLine().trim.toLowerCase == "y"
    
    print("Perform dry run (schema preview only)? (y/N): ")
    val dryRun = StdIn.readLine().trim.toLowerCase == "y"
    
    (pretty, validate, dryRun)
  }
  
  val command: Command[Unit] = Command(
    name = "convert",
    header = "Convert XML file to modern formats with interactive prompts"
  ) {
    Opts.pure {
      try {
        // Interactive input
        val input = interactiveInputFile()
        val output = interactiveOutputFile(input.getName)
        val format = interactiveFormatSelection()
        val (pretty, validate, dryRun) = interactiveConfiguration()
        
        println(s"\n🔄 Converting ${input.getName} to $format...")
        
        // Use default config
        val config = ConfigLoader.defaultConfig(input.getAbsolutePath, output.getAbsolutePath)
        
        // Override config with interactive options
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
          case Success(schema) =>
            println(s"Schema inferred: ${schema.fields.size} fields")
            
            if (dryRun) {
              println("\n📋 Dry run - schema preview:")
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
                case Success(outputFile) =>
                  println(s"✅ Successfully converted to ${outputFile.getAbsolutePath}")
                  
                case Failure(ex) =>
                  println(s"❌ Conversion failed: ${ex.getMessage}")
                  sys.exit(1)
              }
            }
            
          case Failure(ex) =>
            println(s"❌ Schema inference failed: ${ex.getMessage}")
            sys.exit(1)
        }
      } catch {
        case ex: Exception =>
          println(s"❌ Unexpected error: ${ex.getMessage}")
          sys.exit(1)
      }
    }
  }
}
