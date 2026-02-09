package io.slatr.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaSpec extends AnyFlatSpec with Matchers {
  
  "Schema" should "create empty schema" in {
    val schema = Schema.empty("root")
    
    schema.rootElement shouldBe "root"
    schema.fields shouldBe empty
  }
  
  it should "add fields to schema" in {
    val schema = Schema.empty("root")
    val field = Field("name", DataType.StringType, nullable = true, isArray = false)
    
    val updated = schema.withField("name", field)
    
    updated.fields should have size 1
    updated.fields should contain key "name"
  }
  
  it should "merge schemas correctly" in {
    val schema1 = Schema("root", Map(
      "field1" -> Field("field1", DataType.StringType, nullable = true, isArray = false)
    ))
    
    val schema2 = Schema("root", Map(
      "field2" -> Field("field2", DataType.IntType, nullable = true, isArray = false)
    ))
    
    val merged = schema1.merge(schema2)
    
    merged.fields should have size 2
    merged.fields should contain key "field1"
    merged.fields should contain key "field2"
  }
  
  it should "prefer first schema on merge conflicts" in {
    val field1 = Field("common", DataType.StringType, nullable = true, isArray = false)
    val field2 = Field("common", DataType.IntType, nullable = true, isArray = false)
    
    val schema1 = Schema("root", Map("common" -> field1))
    val schema2 = Schema("root", Map("common" -> field2))
    
    val merged = schema1.merge(schema2)
    
    merged.fields("common").dataType shouldBe DataType.StringType
  }
}
