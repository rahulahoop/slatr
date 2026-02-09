package io.slatr.converter

import io.slatr.model.{DataType, Field, Schema}
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type, Types}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

/** Maps slatr Schema to Parquet MessageType */
object ParquetSchemaMapper {
  
  /**
   * Convert slatr Schema to Parquet MessageType
   */
  def toParquetSchema(schema: Schema): MessageType = {
    val fields = schema.fields.map { case (name, field) =>
      fieldToParquetType(name, field)
    }.toList
    
    new MessageType(schema.rootElement, fields: _*)
  }
  
  /**
   * Convert a slatr Field to Parquet Type
   */
  private def fieldToParquetType(name: String, field: Field): Type = {
    val repetition = if (field.nullable) {
      Repetition.OPTIONAL
    } else {
      Repetition.REQUIRED
    }
    
    // Clean field name (remove special characters)
    val cleanName = cleanFieldName(name)
    
    field.dataType match {
      case DataType.StringType =>
        Types.primitive(PrimitiveTypeName.BINARY, repetition)
          .as(LogicalTypeAnnotation.stringType())
          .named(cleanName)
        
      case DataType.IntType =>
        Types.primitive(PrimitiveTypeName.INT32, repetition)
          .named(cleanName)
        
      case DataType.LongType =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .named(cleanName)
        
      case DataType.DoubleType =>
        Types.primitive(PrimitiveTypeName.DOUBLE, repetition)
          .named(cleanName)
        
      case DataType.BooleanType =>
        Types.primitive(PrimitiveTypeName.BOOLEAN, repetition)
          .named(cleanName)
        
      case DataType.DateType =>
        Types.primitive(PrimitiveTypeName.INT32, repetition)
          .as(LogicalTypeAnnotation.dateType())
          .named(cleanName)
        
      case DataType.TimestampType =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .as(LogicalTypeAnnotation.timestampType(
            true, // isAdjustedToUTC
            LogicalTypeAnnotation.TimeUnit.MILLIS
          ))
          .named(cleanName)
        
      case DataType.TimeType =>
        Types.primitive(PrimitiveTypeName.INT64, repetition)
          .as(LogicalTypeAnnotation.timeType(
            true, // isAdjustedToUTC
            LogicalTypeAnnotation.TimeUnit.MILLIS
          ))
          .named(cleanName)
        
      case DataType.DecimalType(precision, scale) =>
        Types.primitive(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
          .as(LogicalTypeAnnotation.decimalType(scale, precision))
          .length(computeMinBytesForPrecision(precision))
          .named(cleanName)
        
      case DataType.ArrayType(elementType) =>
        // For now, treat arrays as repeated primitive fields
        // This is simpler than the full list structure
        val innerRepetition = Repetition.REPEATED
        val innerField = Field(cleanName, elementType, nullable = false, isArray = false)
        fieldToParquetType(cleanName, innerField.copy())
        
      case DataType.StructType(fields) =>
        val structFields = fields.map { case (fieldName, structField) =>
          fieldToParquetType(fieldName, structField)
        }.toList
        Types.buildGroup(repetition)
          .addFields(structFields: _*)
          .named(cleanName)
    }
  }
  
  /**
   * Clean field name to be Parquet-compatible
   * Replace dots and special characters
   */
  private def cleanFieldName(name: String): String = {
    name
      .replace(".", "_")
      .replace("#", "")
      .replace("@", "attr_")
      .replaceAll("[^a-zA-Z0-9_]", "_")
  }
  
  /**
   * Compute minimum bytes needed for decimal precision
   */
  private def computeMinBytesForPrecision(precision: Int): Int = {
    if (precision <= 0) 1
    else Math.ceil((Math.log10(Math.pow(10, precision.toDouble) - 1) + 1) / Math.log10(256)).toInt
  }
}
