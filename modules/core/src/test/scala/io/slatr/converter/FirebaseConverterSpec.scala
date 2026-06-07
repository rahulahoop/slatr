package io.slatr.converter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.jdk.CollectionConverters._

/**
 * Desired API for slatr-d2v: an importable converter that turns an XML file into
 * BigQuery-serializable content maps (java.util.Map[String, Object]) — no BigQuery client,
 * manipulable, and insert-ready via RowToInsert.of(map).
 */
class FirebaseConverterSpec extends AnyFlatSpec with Matchers {

  private val sample = new File("examples/42_Audio.xml")

  "FirebaseConverter.fromXml" should "convert a real ERN file into BQ content maps" in {
    assume(sample.exists(), s"sample not found: ${sample.getAbsolutePath}")

    val rows: Seq[java.util.Map[String, AnyRef]] = FirebaseConverter.fromXml(sample)
    rows should not be empty

    val maps = rows.map(_.asScala)

    // Correlation columns on every row, sourced from the MessageHeader / namespace.
    all(maps.map(_.get("ern_version"))) shouldBe Some("4.2")
    maps.flatMap(_.get("message_id")).distinct should contain("Test1.1")
    maps.flatMap(_.get("element_name")).distinct should contain("ResourceList")
    all(maps.map(_.contains("ingested_at"))) shouldBe true

    // All 21 SoundRecordings preserved in the repeated `fields` struct.
    val fieldNames = rows.flatMap { m =>
      m.get("fields").asInstanceOf[java.util.List[AnyRef]].asScala.map { f =>
        f.asInstanceOf[java.util.Map[String, AnyRef]].get("name").toString
      }
    }
    (0 until 21).foreach { i =>
      withClue(s"missing SoundRecording[$i]: ") {
        fieldNames.exists(_.startsWith(s"SoundRecording[$i]")) shouldBe true
      }
    }
  }

  it should "return mutable maps the caller can manipulate before insert" in {
    assume(sample.exists())

    val row = FirebaseConverter.fromXml(sample).head
    row.put("ingested_by", "backfill-job")
    row.get("ingested_by") shouldBe "backfill-job"
  }
}
