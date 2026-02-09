package io.slatr.converter

import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{BigQueryConfig, Chunk, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser

import java.io.{File, FileInputStream}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/** Writes XML data to BigQuery tables */
class BigQueryWriter(xmlParser: XmlStreamParser) extends LazyLogging {
  
  /**
   * Write XML data to BigQuery table
   */
  def write(
    xmlFile: File,
    schema: Schema,
    config: BigQueryConfig,
    chunk: Option[Chunk] = None
  ): Try[TableId] = Try {
    logger.info(s"Writing ${xmlFile.getName} to BigQuery: ${config.project}.${config.dataset}.${config.table}")
    
    // Initialize BigQuery client
    val bigquery = createBigQueryClient(config)
    
    // Create BigQuery schema
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    logger.debug(s"BigQuery schema: ${bqSchema.getFields.asScala.map(_.getName).mkString(", ")}")
    
    val tableId = TableId.of(config.project, config.dataset, config.table)
    
    // Create or verify table exists
    val table = ensureTable(bigquery, tableId, bqSchema, config)
    
    // Parse XML and prepare rows
    val elements = xmlParser.parse(xmlFile, chunk)
      .getOrElse(throw new Exception("Failed to parse XML"))
    
    // Convert to BigQuery rows and insert
    val rows = elements.map { element =>
      createInsertAllRequest(element, schema, bqSchema)
    }.toList
    
    // Insert in batches
    val batchSize = 500 // BigQuery recommends batches of 500 rows
    val batches = rows.grouped(batchSize).toList
    
    var totalRows = 0
    batches.foreach { batch =>
      val request = InsertAllRequest.newBuilder(tableId)
        .setRows(batch.asJava)
        .build()
      
      val response = bigquery.insertAll(request)
      
      if (response.hasErrors) {
        val errors = response.getInsertErrors.asScala
        logger.error(s"BigQuery insert errors: ${errors.mkString(", ")}")
        throw new Exception(s"BigQuery insert failed with errors: ${errors.head._2.asScala.head.getMessage}")
      }
      
      totalRows += batch.size
      logger.debug(s"Inserted batch of ${batch.size} rows")
    }
    
    logger.info(s"Successfully inserted $totalRows rows into ${config.project}.${config.dataset}.${config.table}")
    
    tableId
  }
  
  /**
   * Create BigQuery client with optional service account credentials
   */
  private def createBigQueryClient(config: BigQueryConfig): BigQuery = {
    val options = config.credentials match {
      case Some(credentialsPath) =>
        logger.info(s"Using service account credentials from: $credentialsPath")
        Using(new FileInputStream(credentialsPath)) { stream =>
          val credentials = ServiceAccountCredentials.fromStream(stream)
          BigQueryOptions.newBuilder()
            .setCredentials(credentials)
            .setProjectId(config.project)
            .build()
        }.get
        
      case None =>
        logger.info("Using Application Default Credentials")
        BigQueryOptions.newBuilder()
          .setProjectId(config.project)
          .build()
    }
    
    options.getService
  }
  
  /**
   * Ensure table exists, create if necessary
   */
  private def ensureTable(
    bigquery: BigQuery,
    tableId: TableId,
    schema: com.google.cloud.bigquery.Schema,
    config: BigQueryConfig
  ): Table = {
    Option(bigquery.getTable(tableId)) match {
      case Some(table) =>
        logger.info(s"Table ${tableId.getTable} exists")
        
        // Handle write mode
        config.writeMode match {
          case WriteMode.Overwrite =>
            logger.info("Overwrite mode: deleting existing data")
            val query = s"DELETE FROM `${config.project}.${config.dataset}.${config.table}` WHERE TRUE"
            val queryConfig = QueryJobConfiguration.newBuilder(query).build()
            bigquery.query(queryConfig)
            
          case WriteMode.ErrorIfExists =>
            throw new Exception(s"Table ${tableId.getTable} already exists and writeMode is ErrorIfExists")
            
          case WriteMode.Append =>
            logger.info("Append mode: adding to existing data")
        }
        
        table
        
      case None =>
        if (config.createTable) {
          logger.info(s"Creating table ${tableId.getTable}")
          
          val tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .build()
          
          val tableInfo = TableInfo.newBuilder(tableId, tableDefinition)
            .build()
          
          bigquery.create(tableInfo)
        } else {
          throw new Exception(s"Table ${tableId.getTable} does not exist and createTable is false")
        }
    }
  }
  
  /**
   * Create InsertAllRequest row from parsed XML element
   */
  private def createInsertAllRequest(
    element: Map[String, Any],
    schema: Schema,
    bqSchema: com.google.cloud.bigquery.Schema
  ): InsertAllRequest.RowToInsert = {
    val content = scala.collection.mutable.Map[String, Any]()
    
    element.foreach { case (key, value) =>
      val cleanKey = cleanFieldName(key)
      
      schema.fields.get(key).foreach { field =>
        try {
          val bqValue = convertValue(value, field.dataType)
          bqValue.foreach { v =>
            content(cleanKey) = v
          }
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to convert field $cleanKey: ${e.getMessage}")
        }
      }
    }
    
    InsertAllRequest.RowToInsert.of(content.asJava)
  }
  
  /**
   * Convert parsed XML value to BigQuery-compatible value
   */
  private def convertValue(value: Any, dataType: io.slatr.model.DataType): Option[Any] = {
    value match {
      case null => None
      
      case list: List[_] =>
        // Handle arrays
        val converted = list.flatMap { item =>
          convertValue(item, dataType)
        }
        if (converted.nonEmpty) Some(converted.asJava) else None
        
      case map: Map[_, _] =>
        // Handle nested structures - extract text content
        val mapValue = map.asInstanceOf[Map[String, Any]]
        mapValue.get("#text") match {
          case Some(text) =>
            convertScalarValue(text.toString, dataType)
          case None =>
            // Complex nested structure - for now skip or serialize as JSON
            None
        }
        
      case scalar =>
        convertScalarValue(scalar.toString, dataType)
    }
  }
  
  /**
   * Convert scalar string value to BigQuery type
   */
  private def convertScalarValue(value: String, dataType: io.slatr.model.DataType): Option[Any] = {
    import io.slatr.model.DataType._
    
    try {
      val result: Any = dataType match {
        case StringType => value
        case IntType => value.toLong // BigQuery uses INT64
        case LongType => value.toLong
        case DoubleType => value.toDouble
        case BooleanType => value.toBoolean
        case TimestampType => parseTimestamp(value)
        case DateType => value // BigQuery accepts ISO 8601 date strings
        case TimeType => value // BigQuery accepts ISO 8601 time strings
        case DecimalType(_, _) => value.toDouble // Convert to FLOAT64
        case _ => value
      }
      Some(result)
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse value '$value' as $dataType: ${e.getMessage}")
        None
    }
  }
  
  /**
   * Parse ISO 8601 timestamp for BigQuery
   */
  private def parseTimestamp(value: String): String = {
    // BigQuery accepts timestamps in ISO 8601 format
    // Try to parse and reformat if needed
    try {
      java.time.Instant.parse(value).toString
    } catch {
      case _: Exception =>
        // If already in correct format or can't parse, return as-is
        value
    }
  }
  
  /**
   * Clean field name to be BigQuery-compatible
   */
  private def cleanFieldName(name: String): String = {
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
      .replaceAll("^_+", "")
      .replaceAll("_+$", "")
      .take(300)
  }
}

object BigQueryWriter {
  def apply(xmlParser: XmlStreamParser): BigQueryWriter = new BigQueryWriter(xmlParser)
}
