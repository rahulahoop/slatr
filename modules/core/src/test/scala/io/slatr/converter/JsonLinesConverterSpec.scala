package io.slatr.converter

import io.slatr.model._
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

class JsonLinesConverterSpec extends AnyFlatSpec with Matchers {
  
  val xmlParser = new XmlStreamParser()
  val converter = JsonLinesConverter(xmlParser)
  val xsdResolver = new XsdResolver(XsdConfig(enabled = false))
  val inferrer = new SchemaInferrer(xsdResolver, xmlParser)
  
  def getTestResource(name: String): File = {
    new File(getClass.getResource(s"/$name").toURI)
  }
  
  "JsonLinesConverter" should "convert XML to JSONL format" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("jsonl-test-", ".jsonl")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.JsonLines,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    val result = converter.convert(file, schema, outputConfig, None)
    
    result.isSuccess shouldBe true
    outputFile.exists() shouldBe true
    outputFile.length() should be > 0L
  }
  
  it should "produce one JSON object per line" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("jsonl-test-lines-", ".jsonl")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.JsonLines,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val lines = Source.fromFile(outputFile).getLines().toList
    
    // Should have 2 lines for 2 books
    lines should have size 2
    
    // Each line should be valid JSON object
    lines.foreach { line =>
      line.trim should startWith("{")
      line.trim should endWith("}")
    }
  }
  
  it should "not pretty-print individual lines" in {
    val file = getTestResource("test-simple.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("jsonl-test-compact-", ".jsonl")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.JsonLines,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val lines = Source.fromFile(outputFile).getLines().toList
    
    // Lines should be compact (no extra whitespace)
    lines.foreach { line =>
      line should not include "  " // No double spaces
    }
  }
  
  it should "handle nested structures in JSONL" in {
    val file = getTestResource("test-nested.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("jsonl-test-nested-", ".jsonl")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.JsonLines,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    val result = converter.convert(file, schema, outputConfig, None)
    
    result.isSuccess shouldBe true
    
    val lines = Source.fromFile(outputFile).getLines().toList
    lines should have size 1
    
    val line = lines.head
    line should include("contact")
  }
  
  it should "have correct file extension" in {
    converter.fileExtension shouldBe "jsonl"
  }
  
  it should "handle single-item lists correctly" in {
    val file = getTestResource("test-single-item.xml")
    val schema = inferrer.infer(file, SchemaConfig(mode = SchemaMode.Auto)).get
    
    val outputFile = File.createTempFile("jsonl-test-single-", ".jsonl")
    outputFile.deleteOnExit()
    
    val outputConfig = OutputConfig(
      format = OutputFormat.JsonLines,
      path = outputFile.getAbsolutePath,
      pretty = false
    )
    
    converter.convert(file, schema, outputConfig, None)
    
    val lines = Source.fromFile(outputFile).getLines().toList
    lines should have size 1
    
    // Should contain arrays even for single items
    val line = lines.head
    line should include("[")
    line should include("]")
  }
}
