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
 * Integration test loading real DDEX ERN (Electronic Release Notification) XML files
 * into BigQuery with the Firebase model, then querying the data back via SQL.
 *
 * Demonstrates:
 * 1. Loading complex, deeply nested music-industry metadata losslessly
 * 2. Discovering and extracting fields with UNNEST over the key-value struct
 * 3. Merging multiple ERN versions into one table (schema evolution)
 */
class DdexToBigQuerySpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

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

  private def query(bq: BigQuery, sql: String): Seq[FieldValueList] =
    bq.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())
      .iterateAll()
      .asScala
      .toSeq

  private def load(bq: BigQuery, file: File, table: String): Unit = {
    val config = BigQueryConfig(
      projectId = "test-project",
      datasetId = "music_metadata",
      tableId = table,
      writeMode = WriteMode.Append,
      credentialsPath = None,
      useFirebaseModel = true
    )
    // Firebase model ignores the declared schema, so an empty one is fine
    val writer = new BigQueryWriter(Schema.empty("NewReleaseMessage"), config, Some(() => bq))
    val _ = writer.writeFromXml(file, XmlStreamParser(), None).get
  }

  "DDEX ERN files" should "load into BigQuery and expose metadata via UNNEST" in {
    val file = new File("examples/42_Audio.xml")
    assume(file.exists(), s"sample not found: ${file.getAbsolutePath}")

    val bq    = client()
    val table = "release_notifications"
    try {
      load(bq, file, table)

      // Rows were created (one per depth-2 element of the message)
      val count = query(bq, s"SELECT COUNT(*) AS c FROM `test-project.music_metadata.$table`")
        .head.get("c").getLongValue
      count should be > 0L

      // Distinct field paths are discoverable via UNNEST
      val fieldNames = query(
        bq,
        s"""
           |SELECT DISTINCT field.name AS name
           |FROM `test-project.music_metadata.$table`, UNNEST(fields) AS field
           |""".stripMargin
      ).map(_.get("name").getStringValue)

      fieldNames should not be empty
      fieldNames.exists(_.startsWith("SoundRecording[")) shouldBe true
      fieldNames.exists(_.contains("ISRC")) shouldBe true

      // Correlation columns are populated from the MessageHeader
      val meta = query(
        bq,
        s"""
           |SELECT message_id, ern_version
           |FROM `test-project.music_metadata.$table`
           |WHERE element_name = 'ResourceList' LIMIT 1
           |""".stripMargin
      ).head
      meta.get("message_id").getStringValue shouldBe "Test1.1"
      meta.get("ern_version").getStringValue shouldBe "4.2"
    } finally {
      try { bq.delete(TableId.of("test-project", "music_metadata", table)); () }
      catch { case _: Exception => }
    }
  }

  it should "merge multiple ERN versions into one table (schema evolution)" in {
    val v42 = new File("examples/42_Audio.xml")
    val v43 = new File("examples/43_Audio.xml")
    assume(v42.exists() && v43.exists())

    val bq    = client()
    val table = "ddex_evolution_test"
    try {
      load(bq, v42, table)
      load(bq, v43, table)

      // Both messages coexist; the Firebase model needs no schema reconciliation
      val versions = query(
        bq,
        s"""
           |SELECT DISTINCT ern_version
           |FROM `test-project.music_metadata.$table`
           |WHERE ern_version IS NOT NULL
           |ORDER BY ern_version
           |""".stripMargin
      ).map(_.get("ern_version").getStringValue)

      versions should contain allOf ("4.2", "4.3")
    } finally {
      try { bq.delete(TableId.of("test-project", "music_metadata", table)); () }
      catch { case _: Exception => }
    }
  }
}
