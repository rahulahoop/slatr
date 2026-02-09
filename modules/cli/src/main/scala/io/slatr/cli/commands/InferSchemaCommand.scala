package io.slatr.cli.commands

import com.monovore.decline._
import io.slatr.cli.config.ConfigLoader
import io.slatr.model.SchemaConfig
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}

import java.io.File
import cats.implicits._

object InferSchemaCommand {
  
  val inputOpt: Opts[File] = Opts.argument[String]("input")
    .mapValidated(s => new File(s).validNel)
    .validate("Input file must exist")(_.exists())
  
  val command: Command[Unit] = Command(
    name = "infer-schema",
    header = "Infer and display schema from XML file"
  ) {
    inputOpt.map { input =>
      
      println(s"Inferring schema from ${input.getName}")
      
      // Initialize components
      val xmlParser = XmlStreamParser()
      val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
      val inferrer = SchemaInferrer(xsdResolver, xmlParser)
      
      // Infer schema
      val schemaResult = inferrer.infer(input, SchemaConfig())
      
      schemaResult match {
        case scala.util.Success(schema) =>
          println(s"\nRoot Element: ${schema.rootElement}")
          println(s"Fields: ${schema.fields.size}\n")
          
          schema.fields.toSeq.sortBy(_._1).foreach { case (name, field) =>
            val arrayMarker = if (field.isArray) "[]" else ""
            val nullableMarker = if (field.nullable) "?" else ""
            println(s"  $name: ${field.dataType}$arrayMarker$nullableMarker")
          }
          
        case scala.util.Failure(ex) =>
          println(s"✗ Schema inference failed: ${ex.getMessage}")
          sys.exit(1)
      }
    }
  }
}
