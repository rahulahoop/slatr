package io.slatr.converter

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{BigQueryConfig, Chunk, DataType, Field, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser

import java.io.{File, FileInputStream}
import java.time.Instant
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/** Writes XML data to BigQuery tables */
class BigQueryWriter(
  schema: Schema,
  config: BigQueryConfig,
  bigQueryFactory: Option[() => BigQuery] = None
) extends LazyLogging {

  /**
   * Write data rows directly to BigQuery table (useful for testing)
   */
  def write(
    rows: Iterator[Map[String, Any]],
    metadata: Map[String, String] = Map.empty
  ): TableId =
    writeRows(rows.map(element => (element, metadata)))

  /**
   * Write rows where each row carries its own top-level metadata (Firebase model only).
   * Used by the DDEX path to attach per-row `element_name` plus shared file metadata.
   */
  private def writeRows(rowsWithMeta: Iterator[(Map[String, Any], Map[String, String])]): TableId = {
    logger.info(
      s"Writing data to BigQuery: ${config.projectId}.${config.datasetId}.${config.tableId}"
    )

    // Initialize BigQuery client
    val bigquery = bigQueryFactory.map(_.apply()).getOrElse(createBigQueryClient(config))

    // In the columnar (traditional) model each row is a depth-2 element's content, so the
    // table columns are that element's children — lift the inferred {elem: Struct} one level.
    val rowSchema = if (config.useFirebaseModel) schema else BigQueryWriter.columnarSchema(schema)

    // Create BigQuery schema
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(rowSchema, config.useFirebaseModel)

    if (config.useFirebaseModel) {
      logger.info("Using Firebase model (array of key-value structs)")
    }

    logger.debug(s"BigQuery schema: ${bqSchema.getFields.asScala.map(_.getName).mkString(", ")}")

    val tableId = TableId.of(config.projectId, config.datasetId, config.tableId)

    // Create or verify table exists
    ensureTable(bigquery, tableId, bqSchema, config)

    // Convert to BigQuery rows
    val rowsList = rowsWithMeta.map { case (element, meta) =>
      if (config.useFirebaseModel) {
        buildFirebaseRow(element, meta)
      } else {
        createInsertAllRequest(element, rowSchema, bqSchema)
      }
    }.toList

    insertRows(bigquery, tableId, rowsList)
    tableId
  }

  /** Insert rows into BigQuery in batches, failing fast on insert errors. */
  private def insertRows(
    bigquery: BigQuery,
    tableId: TableId,
    rows: List[InsertAllRequest.RowToInsert]
  ): Unit = {
    val batchSize = 500 // BigQuery recommends batches of 500 rows
    val batches   = rows.grouped(batchSize).toList

    var totalRows = 0
    batches.foreach { batch =>
      val request = InsertAllRequest
        .newBuilder(tableId)
        .setRows(batch.asJava)
        .build()

      val response = bigquery.insertAll(request)

      if (response.hasErrors) {
        val errors = response.getInsertErrors.asScala
        logger.error(s"BigQuery insert errors: ${errors.mkString(", ")}")
        throw new Exception(
          s"BigQuery insert failed with errors: ${errors.head._2.asScala.head.getMessage}"
        )
      }

      totalRows += batch.size
      logger.debug(s"Inserted batch of ${batch.size} rows")
    }

    logger.info(
      s"Successfully inserted $totalRows rows into ${config.projectId}.${config.datasetId}.${config.tableId}"
    )
  }

  /**
   * Write XML data to BigQuery table (backwards compatibility)
   */
  def writeFromXml(
    xmlFile: File,
    xmlParser: XmlStreamParser,
    chunk: Option[Chunk] = None
  ): Try[TableId] = Try {
    logger.info(
      s"Writing ${xmlFile.getName} to BigQuery: ${config.projectId}.${config.datasetId}.${config.tableId}"
    )

    // Parse XML keeping each depth-2 element's name (e.g. ResourceList, DealList)
    val elements = xmlParser
      .parseNamed(xmlFile, chunk)
      .getOrElse(throw new Exception("Failed to parse XML"))
      .toList

    // File-level correlation metadata, applied to every row of this message
    val fileMetadata = buildFileMetadata(xmlFile, xmlParser, elements)

    // Each row also carries which depth-2 element it came from
    writeRows(elements.iterator.map { case (name, element) =>
      (element, fileMetadata + ("element_name" -> name))
    })
  }

  /**
   * Build the per-file correlation metadata: source path, ERN version, and key
   * MessageHeader fields. These become top-level columns so downstream queries can
   * GROUP BY message_id to reassemble a message from its per-element rows.
   */
  private def buildFileMetadata(
    file: File,
    xmlParser: XmlStreamParser,
    elements: List[(String, Map[String, Any])]
  ): Map[String, String] = {
    val header = elements.collectFirst { case ("MessageHeader", m) => m }
    def hv(path: String*): Option[String] = header.flatMap(h => firstText(h, path.toList))

    Map(
      "file_store_path"          -> Some(file.getAbsolutePath),
      "ern_version"              -> xmlParser.extractErnVersion(file),
      "message_id"               -> hv("MessageId"),
      "message_sender_id"        -> hv("MessageSender", "PartyId"),
      "message_created_datetime" -> hv("MessageCreatedDateTime")
    ).collect { case (k, Some(v)) if v.nonEmpty => k -> v }
  }

  /**
   * Walk a parsed element along `path`, returning the first leaf `#text`.
   * Parser wraps every child element value in a List, so navigate List-of-Map.
   */
  private def firstText(m: Map[String, Any], path: List[String]): Option[String] =
    path match {
      case Nil => m.get("#text").map(_.toString)
      case head :: tail =>
        m.get(head).flatMap {
          case (child: Map[_, _]) :: _ => firstText(child.asInstanceOf[Map[String, Any]], tail)
          case child: Map[_, _]        => firstText(child.asInstanceOf[Map[String, Any]], tail)
          case _                       => None
        }
    }

  /**
   * Create BigQuery client with optional service account credentials
   */
  private def createBigQueryClient(config: BigQueryConfig): BigQuery = {
    val options = config.credentialsPath match {
      case Some(credentialsPath) =>
        logger.info(s"Using service account credentials from: $credentialsPath")
        Using(new FileInputStream(credentialsPath)) { stream =>
          val credentials = ServiceAccountCredentials.fromStream(stream)
          BigQueryOptions
            .newBuilder()
            .setCredentials(credentials)
            .setProjectId(config.projectId)
            .build()
        }.get

      case None =>
        logger.info("Using Application Default Credentials")
        BigQueryOptions
          .newBuilder()
          .setProjectId(config.projectId)
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
  ): Table =
    Option(bigquery.getTable(tableId)) match {
      case Some(table) =>
        logger.info(s"Table ${tableId.getTable} exists")

        // Handle write mode
        config.writeMode match {
          case WriteMode.Overwrite =>
            logger.info("Overwrite mode: deleting existing data")
            val query =
              s"DELETE FROM `${config.projectId}.${config.datasetId}.${config.tableId}` WHERE TRUE"
            val queryConfig = QueryJobConfiguration.newBuilder(query).build()
            bigquery.query(queryConfig)

          case WriteMode.ErrorIfExists =>
            throw new Exception(
              s"Table ${tableId.getTable} already exists and writeMode is ErrorIfExists"
            )

          case WriteMode.Append =>
            logger.info("Append mode: adding to existing data")
        }

        table

      case None =>
        logger.info(s"Creating table ${tableId.getTable}")

        val tableDefinition = StandardTableDefinition
          .newBuilder()
          .setSchema(schema)
          .build()

        val tableInfo = TableInfo
          .newBuilder(tableId, tableDefinition)
          .build()

        bigquery.create(tableInfo)
    }

  /**
   * Create a Firebase-style InsertAllRequest row: the flattened leaf paths in the
   * repeated `fields` struct, plus top-level correlation columns from `metadata`.
   * Format: { fields: [{ name, value }, ...], message_id: ..., ingested_at: ... }
   */
  private def buildFirebaseRow(
    element: Map[String, Any],
    metadata: Map[String, String]
  ): InsertAllRequest.RowToInsert = {
    val fields: java.util.List[java.util.Map[String, Any]] =
      BigQueryWriter.toFirebaseFields(element).map(_.asJava).asJava

    val content = scala.collection.mutable.Map[String, Any]("fields" -> fields)
    metadata.foreach { case (k, v) => content(k) = v }
    content(BigQueryWriter.IngestedAtColumn) = Instant.now().toString

    InsertAllRequest.RowToInsert.of(content.asJava)
  }

  /**
   * Create InsertAllRequest row from parsed XML element
   */
  private def createInsertAllRequest(
    element: Map[String, Any],
    schema: Schema,
    bqSchema: com.google.cloud.bigquery.Schema
  ): InsertAllRequest.RowToInsert = {
    val content = buildRecord(element, schema.fields)
    InsertAllRequest.RowToInsert.of(content)
  }

  /**
   * Build a BigQuery RECORD (java Map) from a parsed element, using `fields` as the schema.
   * The parser wraps every child in a List, so a non-array field collapses its single value;
   * an array field maps over all values; a struct child recurses into a nested RECORD.
   */
  private def buildRecord(
    element: Map[String, Any],
    fields: Map[String, Field]
  ): java.util.Map[String, Any] = {
    val content = scala.collection.mutable.Map[String, Any]()
    element.foreach { case (key, value) =>
      fields.get(key).foreach { field =>
        try {
          convertField(value, field).foreach(v => content(cleanFieldName(key)) = v)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to convert field ${cleanFieldName(key)}: ${e.getMessage}")
        }
      }
    }
    content.asJava
  }

  /**
   * Convert a parsed value for a field, honouring its cardinality (`isArray`).
   * Non-array fields collapse the parser's single-element List wrapper to a scalar/RECORD.
   */
  private def convertField(value: Any, field: Field): Option[Any] =
    if (field.isArray) {
      value match {
        case list: List[_] =>
          val converted = list.flatMap(item => convertLeaf(item, field.dataType))
          if (converted.nonEmpty) Some(converted.asJava) else None
        case other =>
          convertLeaf(other, field.dataType).map(v => List(v).asJava)
      }
    } else {
      value match {
        case list: List[_] => list.headOption.flatMap(item => convertLeaf(item, field.dataType))
        case other         => convertLeaf(other, field.dataType)
      }
    }

  /**
   * Convert a single parsed value to a BigQuery value: scalar from `#text`, or a nested
   * RECORD for a StructType.
   */
  private def convertLeaf(value: Any, dataType: io.slatr.model.DataType): Option[Any] =
    value match {
      case null => None

      case map: Map[_, _] =>
        val mapValue = map.asInstanceOf[Map[String, Any]]
        mapValue.get("#text") match {
          case Some(text) =>
            convertScalarValue(text.toString, dataType)
          case None =>
            dataType match {
              case io.slatr.model.DataType.StructType(structFields) =>
                val record = buildRecord(mapValue, structFields)
                if (record.isEmpty) None else Some(record)
              case _ =>
                None
            }
        }

      case scalar =>
        convertScalarValue(scalar.toString, dataType)
    }

  /**
   * Convert scalar string value to BigQuery type
   */
  private def convertScalarValue(value: String, dataType: io.slatr.model.DataType): Option[Any] = {
    import io.slatr.model.DataType._

    try {
      val result: Any = dataType match {
        case StringType        => value
        case IntType           => value.toLong   // BigQuery uses INT64
        case LongType          => value.toLong
        case DoubleType        => value.toDouble
        case BooleanType       => value.toBoolean
        case TimestampType     => parseTimestamp(value)
        case DateType          => value          // BigQuery accepts ISO 8601 date strings
        case TimeType          => value          // BigQuery accepts ISO 8601 time strings
        case DecimalType(_, _) => value.toDouble // Convert to FLOAT64
        case _                 => value
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
  private def parseTimestamp(value: String): String =
    // BigQuery accepts timestamps in ISO 8601 format
    // Try to parse and reformat if needed
    try
      java.time.Instant.parse(value).toString
    catch {
      case _: Exception =>
        // If already in correct format or can't parse, return as-is
        value
    }

  /**
   * Clean field name to be BigQuery-compatible
   */
  private def cleanFieldName(name: String): String =
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
      .replaceAll("^_+", "")
      .replaceAll("_+$", "")
      .take(300)
}

object BigQueryWriter {

  /**
   * Top-level STRING correlation columns added to every Firebase-model row.
   * Referenced by both the schema mapper and row population so they cannot drift.
   */
  val MetadataStringColumns: Seq[String] = Seq(
    "file_store_path",
    "element_name",
    "message_id",
    "message_sender_id",
    "message_created_datetime",
    "ern_version"
  )

  /** Pipeline ingestion timestamp column (TIMESTAMP). */
  val IngestedAtColumn: String = "ingested_at"

  /**
   * Flatten a parsed XML element into Firebase leaf paths: a list of
   * `Map("name" -> path, "value" -> scalar)`. Fully recursive — keeps every list
   * element (`key[idx]`) and every nested child (`key.child`), so no data is dropped.
   */
  def toFirebaseFields(element: Map[String, Any]): List[Map[String, Any]] =
    element.toList.flatMap { case (key, value) => flatten(key, value) }

  /**
   * Derive the columnar (traditional) row schema. Inference keys fields by the depth-2
   * element name ({book: Struct{...}}), but each row is that element's content, so lift every
   * top-level StructType's children up as columns. Non-struct top-level fields pass through
   * unchanged (e.g. manually-specified flat schemas). On name collisions across element types
   * the first field wins.
   */
  def columnarSchema(schema: Schema): Schema = {
    val lifted = schema.fields.values.foldLeft(Map.empty[String, Field]) { (acc, field) =>
      field.dataType match {
        case DataType.StructType(inner) =>
          inner.foldLeft(acc) { case (m, (name, f)) => if (m.contains(name)) m else m + (name -> f) }
        case _ =>
          if (acc.contains(field.name)) acc else acc + (field.name -> field)
      }
    }
    schema.copy(fields = lifted)
  }

  private def flatten(key: String, value: Any): List[Map[String, Any]] =
    value match {
      case null =>
        List(Map("name" -> key, "value" -> null))

      case list: List[_] =>
        // Keep every element, addressable by index
        list.zipWithIndex.flatMap { case (item, idx) => flatten(s"$key[$idx]", item) }

      case map: Map[_, _] =>
        val m = map.asInstanceOf[Map[String, Any]]
        // Attributes (and any non-text children) live under `key.child`; #text is the leaf
        val rest = (m - "#text").toList.flatMap { case (ck, cv) => flatten(s"$key.$ck", cv) }
        m.get("#text") match {
          case Some(text) => Map("name" -> key, "value" -> text.toString) :: rest
          case None       => rest
        }

      case scalar =>
        List(Map("name" -> key, "value" -> scalar.toString))
    }

  def apply(
    schema: Schema,
    config: BigQueryConfig,
    bigQueryFactory: Option[() => BigQuery] = None
  ): BigQueryWriter = new BigQueryWriter(schema, config, bigQueryFactory)
}
