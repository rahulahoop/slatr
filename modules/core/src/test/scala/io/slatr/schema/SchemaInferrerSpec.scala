package io.slatr.schema

import io.slatr.model._
import io.slatr.parser.XmlStreamParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class SchemaInferrerSpec extends AnyFlatSpec with Matchers {
  
  val xmlParser = new XmlStreamParser()
  val xsdResolver = new XsdResolver(XsdConfig(enabled = false))
  val inferrer = new SchemaInferrer(xsdResolver, xmlParser)
  
  def getTestResource(name: String): File = {
    new File(getClass.getResource(s"/$name").toURI)
  }
  
  "SchemaInferrer" should "infer schema from simple XML in auto mode" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(
      mode = SchemaMode.Auto,
      sampling = SamplingConfig(size = 100)
    )
    
    val result = inferrer.infer(file, config)
    
    result.isSuccess shouldBe true
    val schema = result.get
    
    schema.rootElement shouldBe "catalog"
    schema.fields should not be empty
  }
  
  it should "detect field types from XML content" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    
    // book is a StructType containing typed leaf fields
    schema.fields should contain key "book"
    val bookType = schema.fields("book").dataType.asInstanceOf[DataType.StructType]
    
    bookType.fields("title").dataType shouldBe DataType.StringType
    bookType.fields("year").dataType shouldBe DataType.IntType
    bookType.fields("price").dataType shouldBe DataType.DoubleType
  }
  
  it should "apply manual overrides correctly" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(
      mode = SchemaMode.Hybrid,
      overrides = SchemaOverrides(
        forceArrays = Seq("/catalog/book/title"),
        typeHints = Map("/catalog/book/year" -> "long")
      )
    )
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    // Manual overrides should be applied
    schema.fields should not be empty
  }
  
  it should "infer StructType for nested XML elements" in {
    val file = getTestResource("test-nested.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    schema.rootElement shouldBe "company"
    
    // The top-level "employee" field should exist
    schema.fields should contain key "employee"
    val employeeField = schema.fields("employee")
    
    // employee contains child elements (id, name, contact), so it must be a StructType
    employeeField.dataType shouldBe a [DataType.StructType]
    
    val employeeStruct = employeeField.dataType.asInstanceOf[DataType.StructType]
    employeeStruct.fields should contain key "id"
    employeeStruct.fields should contain key "name"
    employeeStruct.fields should contain key "contact"
    
    // contact is itself nested (email, phone) so it must also be a StructType
    val contactField = employeeStruct.fields("contact")
    contactField.dataType shouldBe a [DataType.StructType]
    
    val contactStruct = contactField.dataType.asInstanceOf[DataType.StructType]
    contactStruct.fields should contain key "email"
    contactStruct.fields should contain key "phone"
  }
  
  it should "infer StructType for DDEX-like deeply nested XML" in {
    val file = getTestResource("test-ddex-nested.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    schema.rootElement shouldBe "NewReleaseMessage"
    
    // Top-level children should be StructType, not StringType
    schema.fields should contain key "MessageHeader"
    schema.fields should contain key "ResourceList"
    
    val headerField = schema.fields("MessageHeader")
    headerField.dataType shouldBe a [DataType.StructType]
    
    val headerStruct = headerField.dataType.asInstanceOf[DataType.StructType]
    headerStruct.fields should contain key "MessageId"
    headerStruct.fields should contain key "MessageSender"
    
    // MessageSender is nested further
    val senderField = headerStruct.fields("MessageSender")
    senderField.dataType shouldBe a [DataType.StructType]
    
    val senderStruct = senderField.dataType.asInstanceOf[DataType.StructType]
    senderStruct.fields should contain key "PartyId"
    senderStruct.fields should contain key "PartyName"
  }
  
  it should "infer integer types from numeric strings" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    
    // Year should be detected as IntType inside the book StructType
    val bookStruct = schema.fields("book").dataType.asInstanceOf[DataType.StructType]
    bookStruct.fields("year").dataType shouldBe DataType.IntType
  }
  
  it should "infer double types from decimal strings" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    
    // Price should be detected as DoubleType inside the book StructType
    val bookStruct = schema.fields("book").dataType.asInstanceOf[DataType.StructType]
    bookStruct.fields("price").dataType shouldBe DataType.DoubleType
  }
  
  it should "respect sampling size configuration" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(
      mode = SchemaMode.Auto,
      sampling = SamplingConfig(size = 1) // Only sample first element
    )
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    // Should still produce valid schema even with limited sampling
    val schema = result.get
    schema.fields should not be empty
  }
}
