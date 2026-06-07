package io.slatr.cli.config

import io.slatr.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ConfigLoaderSpec extends AnyFlatSpec with Matchers {

  "ConfigLoader" should "load the example JSON config" in {
    val file = new File("examples/slatr.example.json")
    assume(file.exists(), s"example config not found: ${file.getAbsolutePath}")

    val cfg = ConfigLoader.loadFromFile(file).get

    cfg.input.path shouldBe "examples/simple.xml"
    cfg.schema.mode shouldBe SchemaMode.Hybrid
    cfg.schema.xsd.timeout shouldBe 30
    cfg.schema.overrides.forceArrays should contain("/catalog/book")
    cfg.schema.overrides.typeHints("/catalog/book/year") shouldBe "int"
    cfg.chunking.chunkSize shouldBe "128MB"
    cfg.output.format shouldBe OutputFormat.Json
    cfg.output.pretty shouldBe true
  }

  it should "decode enum fields from strings and apply defaults for omitted fields" in {
    val cfg = ConfigLoader.parseJson(
      """{"input":{"path":"x.xml"},"schema":{"mode":"xsd"},"chunking":{},"output":{"path":"o.jsonl","format":"jsonl"}}"""
    )

    cfg.schema.mode shouldBe SchemaMode.Xsd       // enum from string
    cfg.output.format shouldBe OutputFormat.JsonLines
    cfg.input.encoding shouldBe "UTF-8"           // default for omitted field
    cfg.logging.level shouldBe "info"             // default for omitted object
    cfg.schema.sampling.size shouldBe 1000        // nested default
  }

  it should "fail on malformed JSON" in {
    ConfigLoader.loadFromFile(File.createTempFile("bad", ".json")).isFailure shouldBe true
  }
}
