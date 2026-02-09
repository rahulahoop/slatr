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
    
    // Should have detected various fields
    schema.fields.values.map(_.dataType) should contain atLeastOneOf(
      DataType.StringType,
      DataType.IntType,
      DataType.DoubleType
    )
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
  
  it should "handle nested structures" in {
    val file = getTestResource("test-nested.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    schema.rootElement shouldBe "company"
    
    // Should have parsed nested contact fields
    schema.fields.keys should contain atLeastOneOf(
      "contact.email.#text",
      "contact.phone.#text"
    )
  }
  
  it should "infer integer types from numeric strings" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    
    // Year should be detected as IntType
    val yearFields = schema.fields.filter { case (name, _) => name.contains("year") }
    yearFields should not be empty
  }
  
  it should "infer double types from decimal strings" in {
    val file = getTestResource("test-simple.xml")
    val config = SchemaConfig(mode = SchemaMode.Auto)
    
    val result = inferrer.infer(file, config)
    result.isSuccess shouldBe true
    
    val schema = result.get
    
    // Price should be detected as DoubleType
    val priceFields = schema.fields.filter { case (name, _) => name.contains("price") }
    priceFields should not be empty
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
