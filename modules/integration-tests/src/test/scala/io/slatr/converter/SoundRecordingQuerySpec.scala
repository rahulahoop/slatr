package io.slatr.converter

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.cloud.NoCredentials
import com.google.cloud.bigquery._
import io.slatr.model.{BigQueryConfig, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.jdk.CollectionConverters._

/**
 * Integration test: load a real DDEX ERN sample (examples/42_Audio.xml, 21 sound
 * recordings) into the BigQuery emulator using the Firebase model, then query the
 * sound recordings back out via SQL.
 *
 * This is the end-to-end guard for the array-drop fix (all 21 recordings must be
 * queryable, not just the first) and for the new top-level correlation columns
 * (message_id, ern_version, element_name) added to the Firebase schema.
 *
 * Requires Docker.
 */
class SoundRecordingQuerySpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

  override val container: GenericContainer = GenericContainer(
    dockerImage = "ghcr.io/goccy/bigquery-emulator:latest",
    exposedPorts = Seq(9050, 9060),
    command = Seq("--project=test-project", "--dataset=music_metadata"),
    waitStrategy = Wait.forLogMessage(".*gRPC server listening.*", 1)
  )

  lazy val restEndpoint: String = s"http://${container.host}:${container.mappedPort(9050)}"

  private def client(): BigQuery =
    BigQueryOptions
      .newBuilder()
      .setHost(restEndpoint)
      .setProjectId("test-project")
      .setCredentials(NoCredentials.getInstance())
      .build()
      .getService

  private val table = "release_notifications"

  private def query(bq: BigQuery, sql: String): Seq[FieldValueList] =
    bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
      .iterateAll()
      .asScala
      .toSeq

  private def load(bq: BigQuery): Unit = {
    val file = new File("examples/42_Audio.xml")
    assume(file.exists(), s"sample file not found: ${file.getAbsolutePath}")

    val config = BigQueryConfig(
      projectId = "test-project",
      datasetId = "music_metadata",
      tableId = table,
      writeMode = WriteMode.Overwrite,
      credentialsPath = None,
      useFirebaseModel = true
    )
    // Firebase model ignores the declared schema, so an empty one is fine here
    val writer = new BigQueryWriter(Schema.empty("NewReleaseMessage"), config, Some(() => bq))
    val _ = writer.writeFromXml(file, XmlStreamParser(), None).get
  }

  "A DDEX ERN sample loaded with the Firebase model" should "expose all 21 sound recording ISRCs via SQL" in {
    val bq = client()
    try {
      load(bq)

      val isrcs = query(
        bq,
        s"""
           |SELECT field.value AS isrc
           |FROM `test-project.music_metadata.$table`, UNNEST(fields) AS field
           |WHERE field.name LIKE 'SoundRecording[%].ResourceId[0].ISRC[0]'
           |ORDER BY isrc
           |""".stripMargin
      ).map(_.get("isrc").getStringValue)

      isrcs should have size 21
      isrcs should contain("JPTO09404900")
      isrcs.distinct should have size 21
    } finally {
      try { bq.delete(TableId.of("test-project", "music_metadata", table)); () }
      catch { case _: Exception => }
    }
  }

  it should "expose message correlation metadata as top-level columns" in {
    val bq = client()
    try {
      load(bq)

      val row = query(
        bq,
        s"""
           |SELECT message_id, ern_version, message_sender_id, element_name
           |FROM `test-project.music_metadata.$table`
           |WHERE element_name = 'ResourceList'
           |LIMIT 1
           |""".stripMargin
      ).head

      row.get("message_id").getStringValue shouldBe "Test1.1"
      row.get("ern_version").getStringValue shouldBe "4.2"
      row.get("message_sender_id").getStringValue shouldBe "PADPIDA2013042401U"
      row.get("element_name").getStringValue shouldBe "ResourceList"
    } finally {
      try { bq.delete(TableId.of("test-project", "music_metadata", table)); () }
      catch { case _: Exception => }
    }
  }
}
