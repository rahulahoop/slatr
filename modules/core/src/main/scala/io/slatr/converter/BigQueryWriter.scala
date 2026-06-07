package io.slatr.converter

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.bigquery._
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{BigQueryConfig, Chunk, DataType, Field, Schema, WriteMode, XmlElement}
import io.slatr.parser.XmlStreamParser

import java.io.{File, FileInputStream}
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
    rows: Iterator[XmlElement],
    metadata: Map[String, String] = Map.empty
  ): TableId =
    writeRows(rows.map(element => (element, metadata)))

  // In the columnar (traditional) model each row is a depth-2 element's content, so the table
  // columns are that element's children — lift the inferred {elem: Struct} one level. The
  // Firebase model ignores the declared schema.
  private lazy val rowSchema: Schema =
    if (config.useFirebaseModel) schema else BigQueryWriter.columnarSchema(schema)

  /**
   * Build rows (each carrying its own top-level metadata) and stream them in. Firebase rows are
   * delegated to [[FirebaseConverter]]; columnar rows are built from the lifted row schema.
   */
  private def writeRows(rowsWithMeta: Iterator[(XmlElement, Map[String, String])]): TableId = {
    val rowsList = rowsWithMeta.map { case (element, meta) =>
      if (config.useFirebaseModel) {
        InsertAllRequest.RowToInsert.of(FirebaseConverter.toMap(element, meta))
      } else {
        createInsertAllRequest(element, rowSchema)
      }
    }.toList
    insert(rowsList)
  }

  /** Create/verify the table for the configured model, then stream the rows in. */
  private def insert(rowsList: List[InsertAllRequest.RowToInsert]): TableId = {
    logger.info(
      s"Writing data to BigQuery: ${config.projectId}.${config.datasetId}.${config.tableId}"
    )

    val bigquery = bigQueryFactory.map(_.apply()).getOrElse(createBigQueryClient(config))
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(rowSchema, config.useFirebaseModel)

    if (config.useFirebaseModel) {
      logger.info("Using Firebase model (array of key-value structs)")
    }
    logger.debug(s"BigQuery schema: ${bqSchema.getFields.asScala.map(_.getName).mkString(", ")}")

    val tableId = TableId.of(config.projectId, config.datasetId, config.tableId)
    ensureTable(bigquery, tableId, bqSchema, config)
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

    if (config.useFirebaseModel) {
      // Firebase rows (with correlation metadata) are built by the importable converter.
      insert(FirebaseConverter.fromXml(xmlFile, xmlParser, chunk).map(InsertAllRequest.RowToInsert.of).toList)
    } else {
      // Columnar model: one row per depth-2 element, no correlation metadata.
      val elements = xmlParser
        .parseNamed(xmlFile, chunk)
        .getOrElse(throw new Exception("Failed to parse XML"))
        .toList
      writeRows(elements.iterator.map { case (_, element) => (element, Map.empty[String, String]) })
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
   * Create InsertAllRequest row from a parsed XML element (columnar model).
   */
  private def createInsertAllRequest(
    element: XmlElement,
    schema: Schema
  ): InsertAllRequest.RowToInsert =
    InsertAllRequest.RowToInsert.of(buildRecord(element, schema.fields))

  /**
   * Build a BigQuery RECORD (java Map) from an element, using `fields` as the schema.
   * Child elements collapse a single occurrence to a scalar/RECORD or map an array; struct
   * children recurse; attributes are written as `@name` columns.
   */
  private def buildRecord(
    element: XmlElement,
    fields: Map[String, Field]
  ): java.util.Map[String, AnyRef] = {
    val content = scala.collection.mutable.Map[String, AnyRef]()

    element.children.foreach { case (name, els) =>
      fields.get(name).foreach { field =>
        try convertField(els, field).foreach(v => content(cleanFieldName(name)) = v)
        catch {
          case e: Exception =>
            logger.warn(s"Failed to convert field ${cleanFieldName(name)}: ${e.getMessage}")
        }
      }
    }

    element.attributes.foreach { case (name, value) =>
      fields.get(s"@$name").foreach { field =>
        convertScalarValue(value, field.dataType).foreach(v => content(cleanFieldName(s"@$name")) = v)
      }
    }

    element.text.foreach { text =>
      fields.get("#text").foreach { field =>
        convertScalarValue(text, field.dataType).foreach(v => content(cleanFieldName("#text")) = v)
      }
    }

    content.asJava
  }

  /**
   * Convert a named child for a field, honouring its cardinality (`isArray`): a non-array
   * field uses the single occurrence; an array field maps over all occurrences.
   */
  private def convertField(elements: List[XmlElement], field: Field): Option[AnyRef] =
    if (field.isArray) {
      val converted = elements.flatMap(e => convertElement(e, field.dataType))
      if (converted.nonEmpty) Some(converted.asJava) else None
    } else {
      elements.headOption.flatMap(e => convertElement(e, field.dataType))
    }

  /**
   * Convert a single element to a BigQuery value: scalar from its text, or a nested RECORD
   * for a StructType.
   */
  private def convertElement(element: XmlElement, dataType: DataType): Option[AnyRef] =
    element.text match {
      case Some(text) =>
        convertScalarValue(text, dataType)
      case None =>
        dataType match {
          case DataType.StructType(structFields) =>
            val record = buildRecord(element, structFields)
            if (record.isEmpty) None else Some(record)
          case _ =>
            None
        }
    }

  /**
   * Convert a scalar string value to a (boxed) BigQuery value.
   */
  private def convertScalarValue(value: String, dataType: DataType): Option[AnyRef] = {
    import io.slatr.model.DataType._

    try {
      val result: AnyRef = dataType match {
        case StringType        => value
        case IntType           => java.lang.Long.valueOf(value.toLong)    // BigQuery uses INT64
        case LongType          => java.lang.Long.valueOf(value.toLong)
        case DoubleType        => java.lang.Double.valueOf(value.toDouble)
        case BooleanType       => java.lang.Boolean.valueOf(value.toBoolean)
        case TimestampType     => parseTimestamp(value)
        case DateType          => value          // BigQuery accepts ISO 8601 date strings
        case TimeType          => value          // BigQuery accepts ISO 8601 time strings
        case DecimalType(_, _) => java.lang.Double.valueOf(value.toDouble) // FLOAT64
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

  def apply(
    schema: Schema,
    config: BigQueryConfig,
    bigQueryFactory: Option[() => BigQuery] = None
  ): BigQueryWriter = new BigQueryWriter(schema, config, bigQueryFactory)
}
