package io.slatr.converter

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{DataType, PostgreSQLConfig, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Types}
import scala.util.{Try, Using}

/**
 * Writes XML data to PostgreSQL tables
 * Supports both traditional columnar schema and Firebase-style JSONB model
 */
class PostgreSQLWriter(
  schema: Schema,
  config: PostgreSQLConfig
) extends LazyLogging {

  /**
   * Write data rows directly to PostgreSQL table
   */
  def write(rows: Iterator[Map[String, Any]]): String = {
    logger.info(s"Writing data to PostgreSQL: ${config.database}.${config.schema}.${config.table}")

    Using.resource(createConnection()) { conn =>
      // Create table if it doesn't exist
      ensureTable(conn)

      // Prepare insert statement
      val insertSql = if (config.useFirebaseModel) {
        createFirebaseInsertSql()
      } else {
        createTraditionalInsertSql()
      }

      Using.resource(conn.prepareStatement(insertSql)) { stmt =>
        var batchCount = 0
        var totalRows = 0

        rows.foreach { row =>
          if (config.useFirebaseModel) {
            setFirebaseParameters(stmt, row)
          } else {
            setTraditionalParameters(stmt, row)
          }
          
          stmt.addBatch()
          batchCount += 1

          if (batchCount >= 500) {
            stmt.executeBatch()
            totalRows += batchCount
            logger.debug(s"Inserted batch of $batchCount rows")
            batchCount = 0
          }
        }

        // Insert remaining rows
        if (batchCount > 0) {
          stmt.executeBatch()
          totalRows += batchCount
        }

        logger.info(s"Successfully inserted $totalRows rows into ${config.table}")
      }

      s"${config.schema}.${config.table}"
    }
  }

  /**
   * Write XML data to PostgreSQL table
   */
  def writeFromXml(
    xmlFile: File,
    xmlParser: XmlStreamParser
  ): Try[String] = Try {
    logger.info(s"Writing ${xmlFile.getName} to PostgreSQL: ${config.database}.${config.schema}.${config.table}")

    val elements = xmlParser
      .parse(xmlFile, None)
      .getOrElse(throw new Exception("Failed to parse XML"))

    write(elements.iterator)
  }

  /**
   * Create database connection
   */
  private def createConnection(): Connection = {
    val url = s"jdbc:postgresql://${config.host}:${config.port}/${config.database}"
    logger.debug(s"Connecting to PostgreSQL: $url")
    
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection(url, config.username, config.password)
  }

  /**
   * Ensure table exists, create if necessary
   */
  private def ensureTable(conn: Connection): Unit = {
    val tableName = s"${config.schema}.${config.table}"
    
    // Check if table exists
    val tableExists = Using.resource(conn.getMetaData.getTables(null, config.schema, config.table, Array("TABLE"))) { rs =>
      rs.next()
    }

    if (tableExists) {
      logger.info(s"Table $tableName exists")
      
      config.writeMode match {
        case WriteMode.Overwrite =>
          logger.info("Overwrite mode: truncating existing data")
          Using.resource(conn.createStatement()) { stmt =>
            stmt.execute(s"TRUNCATE TABLE $tableName")
          }
          
        case WriteMode.ErrorIfExists =>
          throw new Exception(s"Table $tableName already exists and writeMode is ErrorIfExists")
          
        case WriteMode.Append =>
          logger.info("Append mode: adding to existing data")
      }
    } else {
      logger.info(s"Creating table $tableName")
      
      val createTableSql = if (config.useFirebaseModel) {
        createFirebaseTableSql()
      } else {
        createTraditionalTableSql()
      }
      
      Using.resource(conn.createStatement()) { stmt =>
        stmt.execute(createTableSql)
      }
    }
  }

  /**
   * Create Firebase-style table with JSONB column
   */
  private def createFirebaseTableSql(): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${config.schema}.${config.table} (
       |  id SERIAL PRIMARY KEY,
       |  data JSONB NOT NULL,
       |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       |)
       |""".stripMargin
  }

  /**
   * Create traditional table with columns for each field
   */
  private def createTraditionalTableSql(): String = {
    val columns = schema.fields.map { case (name, field) =>
      val cleanName = cleanFieldName(name)
      val sqlType = dataTypeToPostgreSQLType(field.dataType)
      val nullable = if (field.nullable) "NULL" else "NOT NULL"
      s"  $cleanName $sqlType $nullable"
    }.mkString(",\n")

    s"""
       |CREATE TABLE IF NOT EXISTS ${config.schema}.${config.table} (
       |  id SERIAL PRIMARY KEY,
       |$columns,
       |  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
       |)
       |""".stripMargin
  }

  /**
   * Create Firebase-style insert SQL
   */
  private def createFirebaseInsertSql(): String = {
    s"INSERT INTO ${config.schema}.${config.table} (data) VALUES (?::jsonb)"
  }

  /**
   * Create traditional insert SQL
   */
  private def createTraditionalInsertSql(): String = {
    val fieldNames = schema.fields.keys.map(cleanFieldName).mkString(", ")
    val placeholders = schema.fields.keys.map(_ => "?").mkString(", ")
    s"INSERT INTO ${config.schema}.${config.table} ($fieldNames) VALUES ($placeholders)"
  }

  /**
   * Set Firebase model parameters (JSONB)
   */
  private def setFirebaseParameters(stmt: PreparedStatement, row: Map[String, Any]): Unit = {
    // Convert row to JSON format
    val jsonFields = row.flatMap { case (key, value) =>
      convertToFirebaseField(key, value).map { field =>
        s"""{"name":"$key","value":"${escapeJson(field.toString)}"}"""
      }
    }.mkString("[", ",", "]")

    stmt.setString(1, jsonFields)
  }

  /**
   * Set traditional model parameters
   */
  private def setTraditionalParameters(stmt: PreparedStatement, row: Map[String, Any]): Unit = {
    var paramIndex = 1
    
    schema.fields.foreach { case (fieldName, field) =>
      val value = row.get(fieldName)
      
      value match {
        case None | Some(null) =>
          stmt.setNull(paramIndex, getSqlType(field.dataType))
          
        case Some(v) =>
          setParameter(stmt, paramIndex, v, field.dataType)
      }
      
      paramIndex += 1
    }
  }

  /**
   * Set parameter value based on data type
   */
  private def setParameter(stmt: PreparedStatement, index: Int, value: Any, dataType: DataType): Unit = {
    dataType match {
      case DataType.StringType =>
        stmt.setString(index, value.toString)
        
      case DataType.IntType | DataType.LongType =>
        stmt.setLong(index, value.toString.toLong)
        
      case DataType.DoubleType =>
        stmt.setDouble(index, value.toString.toDouble)
        
      case DataType.BooleanType =>
        stmt.setBoolean(index, value.toString.toBoolean)
        
      case DataType.TimestampType =>
        stmt.setTimestamp(index, java.sql.Timestamp.valueOf(value.toString))
        
      case DataType.DateType =>
        stmt.setDate(index, java.sql.Date.valueOf(value.toString))
        
      case DataType.ArrayType(_) =>
        // For arrays, store as JSONB
        val jsonArray = value match {
          case list: List[_] => list.mkString("[\"", "\",\"", "\"]")
          case _ => s"""["$value"]"""
        }
        stmt.setString(index, jsonArray)
        
      case _ =>
        stmt.setString(index, value.toString)
    }
  }

  /**
   * Convert slatr DataType to PostgreSQL type
   */
  private def dataTypeToPostgreSQLType(dataType: DataType): String = dataType match {
    case DataType.StringType => "TEXT"
    case DataType.IntType => "INTEGER"
    case DataType.LongType => "BIGINT"
    case DataType.DoubleType => "DOUBLE PRECISION"
    case DataType.BooleanType => "BOOLEAN"
    case DataType.DateType => "DATE"
    case DataType.TimestampType => "TIMESTAMP"
    case DataType.TimeType => "TIME"
    case DataType.DecimalType(precision, scale) => s"NUMERIC($precision,$scale)"
    case DataType.ArrayType(_) => "JSONB"
    case DataType.StructType(_) => "JSONB"
  }

  /**
   * Get SQL type constant
   */
  private def getSqlType(dataType: DataType): Int = dataType match {
    case DataType.StringType => Types.VARCHAR
    case DataType.IntType => Types.INTEGER
    case DataType.LongType => Types.BIGINT
    case DataType.DoubleType => Types.DOUBLE
    case DataType.BooleanType => Types.BOOLEAN
    case DataType.DateType => Types.DATE
    case DataType.TimestampType => Types.TIMESTAMP
    case DataType.TimeType => Types.TIME
    case DataType.DecimalType(_, _) => Types.NUMERIC
    case _ => Types.VARCHAR
  }

  /**
   * Convert a key-value pair to Firebase field format
   */
  private def convertToFirebaseField(key: String, value: Any): Option[String] = {
    value match {
      case null => Some("null")
      case list: List[_] =>
        list.headOption.map(_.toString)
      case map: Map[_, _] =>
        val mapValue = map.asInstanceOf[Map[String, Any]]
        mapValue.get("#text").map(_.toString).orElse(mapValue.headOption.map(_._2.toString))
      case scalar =>
        Some(scalar.toString)
    }
  }

  /**
   * Clean field name to be PostgreSQL-compatible
   */
  private def cleanFieldName(name: String): String = {
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
      .replaceAll("^_+", "")
      .replaceAll("_+$", "")
      .toLowerCase
      .take(63) // PostgreSQL max identifier length
  }

  /**
   * Escape JSON string
   */
  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  }
}

object PostgreSQLWriter {
  def apply(schema: Schema, config: PostgreSQLConfig): PostgreSQLWriter =
    new PostgreSQLWriter(schema, config)
}
