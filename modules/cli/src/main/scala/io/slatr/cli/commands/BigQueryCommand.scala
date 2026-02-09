package io.slatr.cli.commands

import com.monovore.decline._
import io.slatr.converter.BigQueryWriter
import io.slatr.model._
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}

import java.io.File
import cats.implicits._

object BigQueryCommand {
  
  val inputOpt: Opts[File] = Opts.argument[String]("input")
    .mapValidated(s => new File(s).validNel)
    .validate("Input file must exist")(_.exists())
  
  val projectOpt: Opts[String] = Opts.option[String]("project", short = "p", help = "GCP project ID")
  
  val datasetOpt: Opts[String] = Opts.option[String]("dataset", short = "d", help = "BigQuery dataset name")
  
  val tableOpt: Opts[String] = Opts.option[String]("table", short = "t", help = "BigQuery table name")
  
  val locationOpt: Opts[String] = Opts.option[String]("location", help = "BigQuery location (default: US)")
    .withDefault("US")
  
  val writeModeOpt: Opts[WriteMode] = Opts.option[String]("write-mode", help = "Write mode: append, overwrite, error (default: append)")
    .withDefault("append")
    .map(WriteMode.fromString)
  
  val createTableOpt: Opts[Boolean] = Opts.flag("create-table", help = "Create table if it doesn't exist")
    .orTrue
  
  val credentialsOpt: Opts[Option[String]] = Opts.option[String]("credentials", help = "Path to service account JSON file")
    .orNone
  
  val validateOpt: Opts[Boolean] = Opts.flag("validate", help = "Validate XML against XSD")
    .orFalse
  
  val dryRunOpt: Opts[Boolean] = Opts.flag("dry-run", help = "Preview schema only, don't insert data")
    .orFalse
  
  val command: Command[Unit] = Command(
    name = "to-bigquery",
    header = "Stream XML data directly to BigQuery table"
  ) {
    (inputOpt, projectOpt, datasetOpt, tableOpt, locationOpt, writeModeOpt, createTableOpt, credentialsOpt, validateOpt, dryRunOpt).mapN {
      (input, project, dataset, table, location, writeMode, createTable, credentials, validate, dryRun) =>
        
        println(s"Processing ${input.getName} -> BigQuery: $project.$dataset.$table")
        
        // Initialize components
        val xmlParser = XmlStreamParser()
        val xsdConfig = XsdConfig(enabled = true, validate = validate)
        val xsdResolver = XsdResolver(xsdConfig)
        val inferrer = SchemaInferrer(xsdResolver, xmlParser)
        
        // Infer schema
        println("Inferring schema from XML...")
        val schemaResult = inferrer.infer(input, SchemaConfig())
        
        schemaResult match {
          case scala.util.Success(schema) =>
            println(s"✓ Schema inferred: ${schema.fields.size} fields")
            
            // Show schema preview
            println("\nSchema:")
            schema.fields.toSeq.sortBy(_._1).take(10).foreach { case (name, field) =>
              val arrayMarker = if (field.isArray) "[]" else ""
              val nullableMarker = if (field.nullable) "?" else ""
              println(s"  $name: ${field.dataType}$arrayMarker$nullableMarker")
            }
            if (schema.fields.size > 10) {
              println(s"  ... and ${schema.fields.size - 10} more fields")
            }
            
            if (dryRun) {
              println("\nDry run complete - no data inserted")
            } else {
              // Configure BigQuery
              val bqConfig = BigQueryConfig(
                project = project,
                dataset = dataset,
                table = table,
                location = location,
                writeMode = writeMode,
                createTable = createTable,
                credentials = credentials
              )
              
              // Write to BigQuery
              println("\nInserting data into BigQuery...")
              val writer = BigQueryWriter(xmlParser)
              
              writer.write(input, schema, bqConfig, None) match {
                case scala.util.Success(tableId) =>
                  println(s"✓ Successfully inserted data into ${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}")
                  println(s"\nQuery your data:")
                  println(s"  SELECT * FROM `${tableId.getProject}.${tableId.getDataset}.${tableId.getTable}` LIMIT 10")
                  
                case scala.util.Failure(ex) =>
                  println(s"✗ Failed to insert data: ${ex.getMessage}")
                  ex.printStackTrace()
                  sys.exit(1)
              }
            }
            
          case scala.util.Failure(ex) =>
            println(s"✗ Schema inference failed: ${ex.getMessage}")
            sys.exit(1)
        }
    }
  }
}
