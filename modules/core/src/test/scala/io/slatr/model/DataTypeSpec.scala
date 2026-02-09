package io.slatr.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataTypeSpec extends AnyFlatSpec with Matchers {
  
  "DataType" should "convert XSD string types correctly" in {
    DataType.fromXsdType("string") shouldBe DataType.StringType
    DataType.fromXsdType("xs:string") shouldBe DataType.StringType
  }
  
  it should "convert XSD numeric types correctly" in {
    DataType.fromXsdType("int") shouldBe DataType.IntType
    DataType.fromXsdType("xs:int") shouldBe DataType.IntType
    DataType.fromXsdType("integer") shouldBe DataType.IntType
    DataType.fromXsdType("long") shouldBe DataType.LongType
    DataType.fromXsdType("double") shouldBe DataType.DoubleType
    DataType.fromXsdType("float") shouldBe DataType.DoubleType
  }
  
  it should "convert XSD boolean type correctly" in {
    DataType.fromXsdType("boolean") shouldBe DataType.BooleanType
    DataType.fromXsdType("xs:boolean") shouldBe DataType.BooleanType
  }
  
  it should "convert XSD date/time types correctly" in {
    DataType.fromXsdType("dateTime") shouldBe DataType.TimestampType
    DataType.fromXsdType("xs:dateTime") shouldBe DataType.TimestampType
    DataType.fromXsdType("date") shouldBe DataType.DateType
    DataType.fromXsdType("time") shouldBe DataType.TimeType
  }
  
  it should "convert XSD decimal type correctly" in {
    DataType.fromXsdType("decimal") shouldBe a[DataType.DecimalType]
    DataType.fromXsdType("xs:decimal") shouldBe a[DataType.DecimalType]
  }
  
  it should "default to StringType for unknown types" in {
    DataType.fromXsdType("unknownType") shouldBe DataType.StringType
    DataType.fromXsdType("customType") shouldBe DataType.StringType
  }
}
