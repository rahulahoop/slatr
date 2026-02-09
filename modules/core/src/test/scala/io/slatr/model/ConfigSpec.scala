package io.slatr.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigSpec extends AnyFlatSpec with Matchers {
  
  "SchemaMode" should "parse from string correctly" in {
    SchemaMode.fromString("auto") shouldBe SchemaMode.Auto
    SchemaMode.fromString("xsd") shouldBe SchemaMode.Xsd
    SchemaMode.fromString("manual") shouldBe SchemaMode.Manual
    SchemaMode.fromString("hybrid") shouldBe SchemaMode.Hybrid
  }
  
  it should "default to hybrid for unknown values" in {
    SchemaMode.fromString("unknown") shouldBe SchemaMode.Hybrid
  }
  
  "OutputFormat" should "parse from string correctly" in {
    OutputFormat.fromString("json") shouldBe OutputFormat.Json
    OutputFormat.fromString("jsonl") shouldBe OutputFormat.JsonLines
    OutputFormat.fromString("jsonlines") shouldBe OutputFormat.JsonLines
    OutputFormat.fromString("avro") shouldBe OutputFormat.Avro
    OutputFormat.fromString("parquet") shouldBe OutputFormat.Parquet
  }
  
  it should "default to json for unknown values" in {
    OutputFormat.fromString("unknown") shouldBe OutputFormat.Json
  }
  
  "ChunkingConfig" should "parse chunk size in MB" in {
    val config = ChunkingConfig(chunkSize = "128MB")
    config.chunkSizeBytes shouldBe 128L * 1024 * 1024
  }
  
  it should "parse chunk size in GB" in {
    val config = ChunkingConfig(chunkSize = "1GB")
    config.chunkSizeBytes shouldBe 1L * 1024 * 1024 * 1024
  }
  
  it should "parse chunk size in KB" in {
    val config = ChunkingConfig(chunkSize = "512KB")
    config.chunkSizeBytes shouldBe 512L * 1024
  }
  
  it should "parse raw byte size" in {
    val config = ChunkingConfig(chunkSize = "1048576")
    config.chunkSizeBytes shouldBe 1048576L
  }
}
