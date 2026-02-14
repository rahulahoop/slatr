package io.slatr.converter

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.NoCredentials
import com.google.cloud.bigquery._
import io.slatr.model.{BigQueryConfig, DataType, Field, Schema, WriteMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.wait.strategy.Wait

import java.time.LocalDate
import scala.jdk.CollectionConverters._

class BigQueryIntegrationSpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

  // BigQuery emulator container configuration
  // Note: Requires AMD64 emulation on Apple Silicon (set DOCKER_DEFAULT_PLATFORM=linux/amd64)
  override val container: GenericContainer = GenericContainer(
    dockerImage = "ghcr.io/goccy/bigquery-emulator:latest",
    exposedPorts = Seq(9050, 9060),
    command = Seq("--project=test-project", "--dataset=test_dataset"),
    waitStrategy = Wait.forLogMessage(".*gRPC server listening.*", 1)
  )

  lazy val restEndpoint: String = s"http://${container.host}:${container.mappedPort(9050)}"
  lazy val grpcPort: Int = container.mappedPort(9060)

  def createBigQueryClient(): BigQuery = {
    val options = BigQueryOptions.newBuilder()
      .setHost(restEndpoint)
      .setProjectId("test-project")
      .setCredentials(NoCredentials.getInstance())
      .build()
    
    options.getService
  }

  "BigQueryWriter with emulator" should "create table with inferred schema and insert data" in {
    val client = createBigQueryClient()
    
    try {
      // Define test schema
      val schema = Schema("root", Map(
        "id" -> Field("id", DataType.IntType, nullable = false),
        "name" -> Field("name", DataType.StringType, nullable = false),
        "price" -> Field("price", DataType.DoubleType, nullable = true),
        "created_at" -> Field("created_at", DataType.DateType, nullable = true)
      ))

      // Test data
      val rows = Seq(
        Map[String, Any](
          "id" -> 1,
          "name" -> "Product A",
          "price" -> 19.99,
          "created_at" -> LocalDate.of(2024, 1, 15)
        ),
        Map[String, Any](
          "id" -> 2,
          "name" -> "Product B",
          "price" -> 29.99,
          "created_at" -> LocalDate.of(2024, 1, 20)
        ),
        Map[String, Any](
          "id" -> 3,
          "name" -> "Product C",
          "price" -> null,
          "created_at" -> LocalDate.of(2024, 1, 25)
        )
      )

      // Create BigQueryWriter config
      val config = BigQueryConfig(
        projectId = "test-project",
        datasetId = "test_dataset",
        tableId = "products",
        writeMode = WriteMode.ErrorIfExists,
        credentialsPath = None
      )

      // Write data to BigQuery
      val writer = new BigQueryWriter(
        schema = schema,
        config = config,
        bigQueryFactory = Some(() => client)
      )

      writer.write(rows.iterator)

      // Verify table was created
      val table = client.getTable(TableId.of("test-project", "test_dataset", "products"))
      table should not be null

      // Verify schema
      val bqSchema = table.getDefinition[StandardTableDefinition].getSchema
      bqSchema.getFields.size() shouldBe 4

      val idField = bqSchema.getFields.get("id")
      idField.getType.getStandardType shouldBe StandardSQLTypeName.INT64
      idField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.REQUIRED

      val nameField = bqSchema.getFields.get("name")
      nameField.getType.getStandardType shouldBe StandardSQLTypeName.STRING
      nameField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.REQUIRED

      val priceField = bqSchema.getFields.get("price")
      priceField.getType.getStandardType shouldBe StandardSQLTypeName.FLOAT64
      priceField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.NULLABLE

      val createdAtField = bqSchema.getFields.get("created_at")
      createdAtField.getType.getStandardType shouldBe StandardSQLTypeName.DATE
      createdAtField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.NULLABLE

      // Verify data via query
      val query = "SELECT * FROM `test-project.test_dataset.products` ORDER BY id"
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val resultRows = results.iterateAll().asScala.toSeq

      resultRows should have size 3

      // Verify first row
      resultRows(0).get("id").getLongValue shouldBe 1
      resultRows(0).get("name").getStringValue shouldBe "Product A"
      resultRows(0).get("price").getDoubleValue shouldBe 19.99
      resultRows(0).get("created_at").getStringValue shouldBe "2024-01-15"

      // Verify second row
      resultRows(1).get("id").getLongValue shouldBe 2
      resultRows(1).get("name").getStringValue shouldBe "Product B"
      resultRows(1).get("price").getDoubleValue shouldBe 29.99

      // Verify third row (null price)
      resultRows(2).get("id").getLongValue shouldBe 3
      resultRows(2).get("name").getStringValue shouldBe "Product C"
      resultRows(2).get("price").isNull shouldBe true

    } finally {
      // Cleanup
      try {
        client.delete(TableId.of("test-project", "test_dataset", "products"))
        ()
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  it should "handle append mode correctly" in {
    val client = createBigQueryClient()
    
    try {
      val schema = Schema("root", Map(
        "id" -> Field("id", DataType.IntType, nullable = false),
        "value" -> Field("value", DataType.StringType, nullable = false)
      ))

      val config = BigQueryConfig(
        projectId = "test-project",
        datasetId = "test_dataset",
        tableId = "append_test",
        writeMode = WriteMode.Append,
        credentialsPath = None
      )

      // First write
      val writer1 = new BigQueryWriter(schema, config, Some(() => client))
      writer1.write(Iterator(
        Map[String, Any]("id" -> 1, "value" -> "first")
      ))

      // Second write (append)
      val writer2 = new BigQueryWriter(schema, config, Some(() => client))
      writer2.write(Iterator(
        Map[String, Any]("id" -> 2, "value" -> "second")
      ))

      // Verify both rows exist
      val query = "SELECT COUNT(*) as cnt FROM `test-project.test_dataset.append_test`"
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val count = results.iterateAll().asScala.head.get("cnt").getLongValue
      count shouldBe 2

    } finally {
      try {
        client.delete(TableId.of("test-project", "test_dataset", "append_test"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }

  it should "handle overwrite mode correctly" in {
    val client = createBigQueryClient()
    
    try {
      val schema = Schema("root", Map(
        "id" -> Field("id", DataType.IntType, nullable = false),
        "value" -> Field("value", DataType.StringType, nullable = false)
      ))

      val config = BigQueryConfig(
        projectId = "test-project",
        datasetId = "test_dataset",
        tableId = "overwrite_test",
        writeMode = WriteMode.Overwrite,
        credentialsPath = None
      )

      // First write
      val writer1 = new BigQueryWriter(schema, config, Some(() => client))
      writer1.write(Iterator(
        Map[String, Any]("id" -> 1, "value" -> "first"),
        Map[String, Any]("id" -> 2, "value" -> "second")
      ))

      // Second write (overwrite)
      val configOverwrite = config.copy(writeMode = WriteMode.Overwrite)
      val writer2 = new BigQueryWriter(schema, configOverwrite, Some(() => client))
      writer2.write(Iterator(
        Map[String, Any]("id" -> 3, "value" -> "third")
      ))

      // Verify only new data exists
      val query = "SELECT * FROM `test-project.test_dataset.overwrite_test` ORDER BY id"
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val rows = results.iterateAll().asScala.toSeq

      rows should have size 1
      rows(0).get("id").getLongValue shouldBe 3
      rows(0).get("value").getStringValue shouldBe "third"

    } finally {
      try {
        client.delete(TableId.of("test-project", "test_dataset", "overwrite_test"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }

  it should "handle arrays correctly" in {
    val client = createBigQueryClient()
    
    try {
      val schema = Schema("root", Map(
        "id" -> Field("id", DataType.IntType, nullable = false),
        "tags" -> Field("tags", DataType.ArrayType(DataType.StringType), nullable = true, isArray = true)
      ))

      val config = BigQueryConfig(
        projectId = "test-project",
        datasetId = "test_dataset",
        tableId = "array_test",
        writeMode = WriteMode.ErrorIfExists,
        credentialsPath = None
      )

      val writer = new BigQueryWriter(schema, config, Some(() => client))
      writer.write(Iterator(
        Map[String, Any]("id" -> 1, "tags" -> Seq("scala", "bigquery", "testing"))
      ))

      // Verify schema has REPEATED mode
      val table = client.getTable(TableId.of("test-project", "test_dataset", "array_test"))
      val tagsField = table.getDefinition[StandardTableDefinition].getSchema.getFields.get("tags")
      tagsField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.REPEATED

      // Verify data
      val query = "SELECT id, tags FROM `test-project.test_dataset.array_test`"
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val row = results.iterateAll().asScala.head
      
      row.get("id").getLongValue shouldBe 1
      val tags = row.get("tags").getRepeatedValue.asScala.map(_.getStringValue).toSeq
      tags shouldBe Seq("scala", "bigquery", "testing")

    } finally {
      try {
        client.delete(TableId.of("test-project", "test_dataset", "array_test"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }

  it should "handle Firebase model correctly" in {
    val client = createBigQueryClient()
    
    try {
      // Using a simple schema - Firebase model will flatten it
      val schema = Schema("root", Map(
        "id" -> Field("id", DataType.IntType, nullable = false),
        "name" -> Field("name", DataType.StringType, nullable = false),
        "status" -> Field("status", DataType.StringType, nullable = true)
      ))

      val config = BigQueryConfig(
        projectId = "test-project",
        datasetId = "test_dataset",
        tableId = "firebase_test",
        writeMode = WriteMode.ErrorIfExists,
        credentialsPath = None,
        useFirebaseModel = true
      )

      val writer = new BigQueryWriter(schema, config, Some(() => client))
      writer.write(Iterator(
        Map[String, Any]("id" -> 1, "name" -> "Item A", "status" -> "active"),
        Map[String, Any]("id" -> 2, "name" -> "Item B", "status" -> "inactive")
      ))

      // Verify Firebase schema structure
      val table = client.getTable(TableId.of("test-project", "test_dataset", "firebase_test"))
      val bqSchema = table.getDefinition[StandardTableDefinition].getSchema
      
      bqSchema.getFields.size() shouldBe 1
      val fieldsField = bqSchema.getFields.get("fields")
      fieldsField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.REPEATED
      fieldsField.getType.getStandardType shouldBe StandardSQLTypeName.STRUCT

      // Verify nested structure
      val structFields = fieldsField.getSubFields
      structFields.size() shouldBe 2
      structFields.get("name").getType.getStandardType shouldBe StandardSQLTypeName.STRING
      structFields.get("value").getType.getStandardType shouldBe StandardSQLTypeName.STRING

      // Query the data with UNNEST
      val query = """
        SELECT 
          (SELECT value FROM UNNEST(fields) WHERE name = 'id') as id,
          (SELECT value FROM UNNEST(fields) WHERE name = 'name') as name,
          (SELECT value FROM UNNEST(fields) WHERE name = 'status') as status
        FROM `test-project.test_dataset.firebase_test`
        ORDER BY id
      """
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val rows = results.iterateAll().asScala.toSeq

      rows should have size 2
      rows(0).get("id").getStringValue shouldBe "1"
      rows(0).get("name").getStringValue shouldBe "Item A"
      rows(0).get("status").getStringValue shouldBe "active"

      rows(1).get("id").getStringValue shouldBe "2"
      rows(1).get("name").getStringValue shouldBe "Item B"
      rows(1).get("status").getStringValue shouldBe "inactive"

    } finally {
      try {
        client.delete(TableId.of("test-project", "test_dataset", "firebase_test"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }
}
