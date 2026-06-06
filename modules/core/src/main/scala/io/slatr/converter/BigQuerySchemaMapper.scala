package io.slatr.converter

import com.google.cloud.bigquery.{
  Field => BQField,
  Schema => BQSchema,
  StandardSQLTypeName
}
import io.slatr.model.{DataType, Field, Schema}

import scala.jdk.CollectionConverters._

/** Maps slatr Schema to BigQuery Schema */
object BigQuerySchemaMapper {

  /**
   * Convert slatr Schema to BigQuery Schema
   */
  def toBigQuerySchema(schema: Schema, useFirebaseModel: Boolean = false): BQSchema = {
    if (useFirebaseModel) {
      toFirebaseSchema()
    } else {
      val fields = schema.fields.map { case (name, field) =>
        fieldToBigQueryField(name, field)
      }.toList

      BQSchema.of(fields.asJava)
    }
  }

  /**
   * Create Firebase-style schema: a repeated key-value struct holding flattened leaf
   * paths, plus top-level correlation columns (message_id, file_store_path, etc.).
   * The struct avoids column limits / allows schema evolution; the correlation columns
   * let downstream queries join and reassemble messages.
   */
  private def toFirebaseSchema(): BQSchema = {
    val nameField = BQField
      .newBuilder("name", StandardSQLTypeName.STRING)
      .setMode(BQField.Mode.REQUIRED)
      .build()

    val valueField = BQField
      .newBuilder("value", StandardSQLTypeName.STRING)
      .setMode(BQField.Mode.NULLABLE)
      .build()

    val fieldStruct = BQField
      .newBuilder("fields", StandardSQLTypeName.STRUCT, nameField, valueField)
      .setMode(BQField.Mode.REPEATED)
      .build()

    val metadataColumns = BigQueryWriter.MetadataStringColumns.map { name =>
      BQField.newBuilder(name, StandardSQLTypeName.STRING).setMode(BQField.Mode.NULLABLE).build()
    }

    val ingestedAt = BQField
      .newBuilder(BigQueryWriter.IngestedAtColumn, StandardSQLTypeName.TIMESTAMP)
      .setMode(BQField.Mode.NULLABLE)
      .build()

    BQSchema.of((fieldStruct +: metadataColumns :+ ingestedAt).asJava)
  }

  /**
   * Convert a slatr Field to BigQuery Field
   */
  private def fieldToBigQueryField(name: String, field: Field): BQField = {
    val cleanName = cleanFieldName(name)
    val mode = if (field.isArray) {
      BQField.Mode.REPEATED
    } else if (field.nullable) {
      BQField.Mode.NULLABLE
    } else {
      BQField.Mode.REQUIRED
    }

    val bqType = dataTypeToBigQueryType(field.dataType)

    field.dataType match {
      case DataType.StructType(fields) =>
        // Create RECORD type with nested fields
        val nestedFields = fields.map { case (fieldName, structField) =>
          fieldToBigQueryField(fieldName, structField)
        }.toSeq

        BQField
          .newBuilder(cleanName, StandardSQLTypeName.STRUCT, nestedFields: _*)
          .setMode(mode)
          .build()

      case _ =>
        BQField
          .newBuilder(cleanName, bqType)
          .setMode(mode)
          .build()
    }
  }

  /**
   * Map slatr DataType to BigQuery StandardSQLTypeName
   */
  private def dataTypeToBigQueryType(dataType: DataType): StandardSQLTypeName =
    dataType match {
      case DataType.StringType                    => StandardSQLTypeName.STRING
      case DataType.IntType                       => StandardSQLTypeName.INT64
      case DataType.LongType                      => StandardSQLTypeName.INT64
      case DataType.DoubleType                    => StandardSQLTypeName.FLOAT64
      case DataType.BooleanType                   => StandardSQLTypeName.BOOL
      case DataType.DateType                      => StandardSQLTypeName.DATE
      case DataType.TimestampType                 => StandardSQLTypeName.TIMESTAMP
      case DataType.TimeType                      => StandardSQLTypeName.TIME
      case DataType.DecimalType(precision, scale) => StandardSQLTypeName.NUMERIC
      case DataType.ArrayType(elementType)        =>
        // Arrays are handled by REPEATED mode
        dataTypeToBigQueryType(elementType)
      case DataType.StructType(_) => StandardSQLTypeName.STRUCT
    }

  /**
   * Clean field name to be BigQuery-compatible BigQuery field names must contain only letters,
   * numbers, and underscores
   */
  private def cleanFieldName(name: String): String =
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
      .replaceAll("^_+", "") // Remove leading underscores
      .replaceAll("_+$", "") // Remove trailing underscores
      .take(300)             // BigQuery max field name length
}
