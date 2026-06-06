package io.slatr.converter

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{Chunk, DataType, OutputConfig, Schema, XmlElement}
import io.slatr.parser.XmlStreamParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.GroupWriteSupport

import java.io.File
import scala.util.Try

/** Converts XML to Parquet format */
class ParquetConverter(xmlParser: XmlStreamParser) extends Converter with LazyLogging {
  
  override def fileExtension: String = "parquet"
  
  override def convert(
    xmlFile: File,
    schema: Schema,
    outputConfig: OutputConfig,
    chunk: Option[Chunk]
  ): Try[File] = Try {
    logger.info(s"Converting ${xmlFile.getName} to Parquet")
    
    // Create Parquet schema
    val parquetSchema = ParquetSchemaMapper.toParquetSchema(schema)
    logger.debug(s"Parquet schema: ${parquetSchema.toString}")
    
    val outputFile = new File(outputConfig.path)
    val outputPath = new Path(outputFile.getAbsolutePath)
    
    // Configure Hadoop (minimal config for local filesystem)
    val conf = new Configuration()
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("hadoop.security.authentication", "simple")
    conf.set("hadoop.security.authorization", "false")
    
    // Disable UserGroupInformation security features to avoid security manager issues
    System.setProperty("hadoop.home.dir", "/")
    
    GroupWriteSupport.setSchema(parquetSchema, conf)
    
    // Determine compression
    val compression = outputConfig.compression match {
      case Some("snappy") => CompressionCodecName.SNAPPY
      case Some("gzip") => CompressionCodecName.GZIP
      case Some("lzo") => CompressionCodecName.LZO
      case Some("brotli") => CompressionCodecName.BROTLI
      case Some("zstd") => CompressionCodecName.ZSTD
      case Some("none") => CompressionCodecName.UNCOMPRESSED
      case None => CompressionCodecName.SNAPPY // Default
      case Some(other) =>
        logger.warn(s"Unknown compression codec: $other, using SNAPPY")
        CompressionCodecName.SNAPPY
    }
    
    // Create Parquet writer using builder pattern
    val writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(outputPath)
      .withType(parquetSchema)
      .withCompressionCodec(compression)
      .withConf(conf)
      .build()
    
    try {
      val groupFactory = new SimpleGroupFactory(parquetSchema)
      
      // Parse XML and write to Parquet
      val elements = xmlParser.parse(xmlFile, chunk)
        .getOrElse(throw new Exception("Failed to parse XML"))
      
      var count = 0
      elements.foreach { element =>
        val group = createGroup(element, groupFactory, schema)
        writer.write(group)
        count += 1
      }
      
      logger.info(s"Successfully wrote $count records to ${outputFile.getAbsolutePath}")
    } finally {
      writer.close()
    }
    
    outputFile
  }
  
  /**
   * Create a Parquet Group from an element: write child elements and attributes that match
   * the schema. Only leaf (text) values are written; complex nested structures are skipped.
   */
  private def createGroup(
    element: XmlElement,
    groupFactory: SimpleGroupFactory,
    schema: Schema
  ): Group = {
    val group = groupFactory.newGroup()

    element.children.foreach { case (name, els) =>
      schema.fields.get(name).foreach { field =>
        try writeElements(group, cleanFieldName(name), els, field.dataType)
        catch {
          case e: Exception =>
            logger.warn(s"Failed to write field ${cleanFieldName(name)}: ${e.getMessage}")
        }
      }
    }

    element.attributes.foreach { case (name, value) =>
      schema.fields.get(s"@$name").foreach { field =>
        try writeScalarValue(group, cleanFieldName(s"@$name"), value, field.dataType)
        catch {
          case e: Exception =>
            logger.warn(s"Failed to write attribute ${cleanFieldName(s"@$name")}: ${e.getMessage}")
        }
      }
    }

    group
  }

  /** Write a (possibly repeated) child element to the group. */
  private def writeElements(
    group: Group,
    fieldName: String,
    elements: List[XmlElement],
    dataType: DataType
  ): Unit =
    elements.foreach { e =>
      e.text match {
        case Some(text) => writeScalarValue(group, fieldName, text, dataType)
        case None       => logger.debug(s"Complex nested structure for field $fieldName, skipping")
      }
    }
  
  /**
   * Write a scalar value to Parquet Group
   */
  private def writeScalarValue(group: Group, fieldName: String, value: String, dataType: io.slatr.model.DataType): Unit = {
    import io.slatr.model.DataType._
    
    try {
      val _ = dataType match {
        case StringType =>
          group.append(fieldName, value)
          
        case IntType =>
          group.append(fieldName, value.toInt)
          
        case LongType =>
          group.append(fieldName, value.toLong)
          
        case DoubleType =>
          group.append(fieldName, value.toDouble)
          
        case BooleanType =>
          group.append(fieldName, value.toBoolean)
          
        case TimestampType =>
          // Parse ISO 8601 timestamp
          // For now, just store as long (millis since epoch)
          val millis = parseTimestamp(value)
          group.append(fieldName, millis)
          
        case DateType =>
          // Store as days since epoch
          val days = parseDate(value)
          group.append(fieldName, days)
          
        case TimeType =>
          // Store as millis since midnight
          val millis = parseTime(value)
          group.append(fieldName, millis)
          
        case DecimalType(precision, scale) =>
          // For now, store as double (proper decimal support requires BigDecimal)
          group.append(fieldName, value.toDouble)
          
        case ArrayType(elementType) =>
          // Should be handled by parent writeValue
          logger.warn(s"Unexpected ArrayType at scalar level for field $fieldName")
          
        case StructType(_) =>
          // Should be handled by parent writeValue
          logger.warn(s"Unexpected StructType at scalar level for field $fieldName")
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse value '$value' as $dataType for field $fieldName: ${e.getMessage}")
    }
  }
  
  /**
   * Parse ISO 8601 timestamp to milliseconds since epoch
   */
  private def parseTimestamp(value: String): Long = {
    try {
      java.time.Instant.parse(value).toEpochMilli
    } catch {
      case _: Exception =>
        // Fallback: try as long
        value.toLong
    }
  }
  
  /**
   * Parse date to days since epoch
   */
  private def parseDate(value: String): Int = {
    try {
      val localDate = java.time.LocalDate.parse(value)
      localDate.toEpochDay.toInt
    } catch {
      case _: Exception =>
        // Fallback: try as int
        value.toInt
    }
  }
  
  /**
   * Parse time to milliseconds since midnight
   */
  private def parseTime(value: String): Long = {
    try {
      val localTime = java.time.LocalTime.parse(value)
      localTime.toNanoOfDay / 1000000 // Convert nanos to millis
    } catch {
      case _: Exception =>
        // Fallback: try as long
        value.toLong
    }
  }
  
  /**
   * Clean field name to be Parquet-compatible
   */
  private def cleanFieldName(name: String): String = {
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
  }
}

object ParquetConverter {
  def apply(xmlParser: XmlStreamParser): ParquetConverter = new ParquetConverter(xmlParser)
}
