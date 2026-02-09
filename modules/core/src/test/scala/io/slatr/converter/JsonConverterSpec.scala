package io.slatr.converter

import io.slatr.model._
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

class JsonConverterSpec extends AnyFlatSpec with Matchers {
  
  val xmlParser = new XmlStreamParser()
  val converter = JsonConverter(xmlParser)
  val xsdResolver = new XsdResolver(XsdConfig(enabled = false))
  val inferrer = new SchemaInferrer(xsdResolver, xmlParser)
  
  def getTestResource(name: String): File = {
    new File(getClass.getResource(s"/$name").toURI)
  }
  
  "JsonConverter" should "convert simple XML to JSON" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("json-test-", ".json")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.Json,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    val result = converter.convert(file, schema, outputConfig, None)
    
    result.isSuccess shouldBe true
    outputFile.exists() shouldBe true
    outputFile.length() should be > 0L
  }
  
  it should "produce valid JSON output" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("json-test-", ".json")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.Json,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val jsonContent = Source.fromFile(outputFile).mkString
    
    // Should be valid JSON (starts with [ or {)
    jsonContent.trim should (startWith("[") or startWith("{"))
  }
  
  it should "pretty-print JSON when configured" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("json-test-pretty-", ".json")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.Json,
      path = outputFile.getAbsolutePath,
      pretty = true
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val jsonContent = Source.fromFile(outputFile).mkString
    
    // Pretty-printed JSON should contain newlines
    jsonContent should include("\n")
  }
  
  it should "handle nested structures" in {
    val file = getTestResource("test-nested.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("json-test-nested-", ".json")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.Json,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    val result = converter.convert(file, schema, outputConfig, None)
    
    result.isSuccess shouldBe true
    
    val jsonContent = Source.fromFile(outputFile).mkString
    jsonContent should include("contact")
  }
  
  it should "convert arrays correctly" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("json-test-arrays-", ".json")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.Json,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val jsonContent = Source.fromFile(outputFile).mkString
    
    // Should have array brackets
    jsonContent should include("[")
    jsonContent should include("]")
  }
  
  it should "have correct file extension" in {
    converter.fileExtension shouldBe "json"
  }
}
