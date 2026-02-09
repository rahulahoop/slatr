package io.slatr.converter

import com.google.cloud.bigquery.StandardSQLTypeName
import io.slatr.model.{DataType, Field, Schema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class BigQuerySchemaMapperSpec extends AnyFlatSpec with Matchers {
  
  "BigQuerySchemaMapper" should "convert simple schema to BigQuery schema" in {
    val schema = Schema("root", Map(
      "name" -> Field("name", DataType.StringType, nullable = true, isArray = false),
      "age" -> Field("age", DataType.IntType, nullable = true, isArray = false),
      "active" -> Field("active", DataType.BooleanType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    
    bqSchema.getFields should have size 3
    bqSchema.getFields.asScala.map(_.getName) should contain allOf("name", "age", "active")
  }
  
  it should "map string types correctly" in {
    val schema = Schema("root", Map(
      "text" -> Field("text", DataType.StringType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    val field = bqSchema.getFields.get(0)
    
    field.getName shouldBe "text"
    field.getType.getStandardType shouldBe StandardSQLTypeName.STRING
  }
  
  it should "map numeric types correctly" in {
    val schema = Schema("root", Map(
      "int_field" -> Field("int_field", DataType.IntType, nullable = true, isArray = false),
      "long_field" -> Field("long_field", DataType.LongType, nullable = true, isArray = false),
      "double_field" -> Field("double_field", DataType.DoubleType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    
    bqSchema.getFields.asScala.find(_.getName == "int_field").get.getType.getStandardType shouldBe StandardSQLTypeName.INT64
    bqSchema.getFields.asScala.find(_.getName == "long_field").get.getType.getStandardType shouldBe StandardSQLTypeName.INT64
    bqSchema.getFields.asScala.find(_.getName == "double_field").get.getType.getStandardType shouldBe StandardSQLTypeName.FLOAT64
  }
  
  it should "map date/time types correctly" in {
    val schema = Schema("root", Map(
      "date_field" -> Field("date_field", DataType.DateType, nullable = true, isArray = false),
      "timestamp_field" -> Field("timestamp_field", DataType.TimestampType, nullable = true, isArray = false),
      "time_field" -> Field("time_field", DataType.TimeType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    
    bqSchema.getFields.asScala.find(_.getName == "date_field").get.getType.getStandardType shouldBe StandardSQLTypeName.DATE
    bqSchema.getFields.asScala.find(_.getName == "timestamp_field").get.getType.getStandardType shouldBe StandardSQLTypeName.TIMESTAMP
    bqSchema.getFields.asScala.find(_.getName == "time_field").get.getType.getStandardType shouldBe StandardSQLTypeName.TIME
  }
  
  it should "handle nullable vs required fields" in {
    val schema = Schema("root", Map(
      "required" -> Field("required", DataType.StringType, nullable = false, isArray = false),
      "optional" -> Field("optional", DataType.StringType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    
    val requiredField = bqSchema.getFields.asScala.find(_.getName == "required").get
    val optionalField = bqSchema.getFields.asScala.find(_.getName == "optional").get
    
    requiredField.getMode.name() shouldBe "REQUIRED"
    optionalField.getMode.name() shouldBe "NULLABLE"
  }
  
  it should "handle array fields with REPEATED mode" in {
    val schema = Schema("root", Map(
      "tags" -> Field("tags", DataType.StringType, nullable = true, isArray = true)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    val field = bqSchema.getFields.get(0)
    
    field.getMode.name() shouldBe "REPEATED"
    field.getType.getStandardType shouldBe StandardSQLTypeName.STRING
  }
  
  it should "clean field names to be BigQuery-compatible" in {
    val schema = Schema("root", Map(
      "#text" -> Field("#text", DataType.StringType, nullable = true, isArray = false),
      "@attr" -> Field("@attr", DataType.StringType, nullable = true, isArray = false),
      "field.with.dots" -> Field("field.with.dots", DataType.StringType, nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    val fieldNames = bqSchema.getFields.asScala.map(_.getName).toSet
    
    fieldNames should contain("text")
    fieldNames should contain("attr_attr")
    fieldNames should contain("field_with_dots")
  }
  
  it should "handle decimal types" in {
    val schema = Schema("root", Map(
      "amount" -> Field("amount", DataType.DecimalType(10, 2), nullable = true, isArray = false)
    ))
    
    val bqSchema = BigQuerySchemaMapper.toBigQuerySchema(schema)
    val field = bqSchema.getFields.get(0)
    
    field.getType.getStandardType shouldBe StandardSQLTypeName.NUMERIC
  }
}
