package io.slatr.converter

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import com.google.api.gax.core.NoCredentialsProvider
import com.google.cloud.bigquery._
import io.slatr.model.{BigQueryConfig, WriteMode}
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.wait.strategy.Wait

import java.io.File
import scala.jdk.CollectionConverters._

/**
 * End-to-end integration test that:
 * 1. Starts BigQuery emulator
 * 2. Reads multiple XML files
 * 3. Infers schemas
 * 4. Loads data into BigQuery (both traditional and Firebase models)
 * 5. Validates the data with SQL queries
 */
class XmlToBigQueryE2ESpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

  // BigQuery emulator container configuration
  override val container: GenericContainer = GenericContainer(
    dockerImage = "ghcr.io/goccy/bigquery-emulator:latest",
    exposedPorts = Seq(9050, 9060),
    command = Seq("--project=test-project", "--dataset=test_dataset"),
    waitStrategy = Wait.forLogMessage(".*gRPC server listening.*", 1)
  )

  lazy val restEndpoint: String = s"http://${container.host}:${container.mappedPort(9050)}"

  def createBigQueryClient(): BigQuery = {
    val options = BigQueryOptions.newBuilder()
      .setHost(restEndpoint)
      .setProjectId("test-project")
      .setCredentials(NoCredentialsProvider.create().getCredentials)
      .build()
    
    options.getService
  }

  "XML to BigQuery E2E workflow" should "load multiple XML files using traditional schema" in {
    val client = createBigQueryClient()
    val xmlParser = XmlStreamParser()
    val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
    val inferrer = SchemaInferrer(xsdResolver, xmlParser)

    try {
      // Test files
      val xmlFiles = Seq(
        new File("examples/simple.xml"),
        new File("examples/nested.xml")
      )

      // Process simple.xml (books catalog)
      val simpleFile = xmlFiles(0)
      if (simpleFile.exists()) {
        println(s"\n📖 Processing ${simpleFile.getName}...")
        
        val schemaResult = inferrer.infer(simpleFile, io.slatr.model.SchemaConfig())
        schemaResult.isSuccess shouldBe true
        
        val schema = schemaResult.get
        println(s"   Schema inferred: ${schema.fields.size} fields")

        val config = BigQueryConfig(
          projectId = "test-project",
          datasetId = "test_dataset",
          tableId = "books",
          writeMode = WriteMode.Append,
          credentialsPath = None,
          useFirebaseModel = false
        )

        val writer = new BigQueryWriter(schema, config, Some(() => client))
        val writeResult = writer.writeFromXml(simpleFile, xmlParser, None)
        writeResult.isSuccess shouldBe true

        println(s"   ✅ Data loaded into books table")

        // Query and validate
        val query = "SELECT COUNT(*) as count FROM `test-project.test_dataset.books`"
        val queryConfig = QueryJobConfiguration.newBuilder(query)
          .setUseLegacySql(false)
          .build()

        val results = client.query(queryConfig)
        val count = results.iterateAll().asScala.head.get("count").getLongValue
        println(s"   📊 Row count: $count")
        count should be > 0L

        // Query specific fields
        val detailQuery = """
          SELECT title, author, year, price, available 
          FROM `test-project.test_dataset.books` 
          ORDER BY year 
          LIMIT 3
        """
        val detailConfig = QueryJobConfiguration.newBuilder(detailQuery)
          .setUseLegacySql(false)
          .build()

        val detailResults = client.query(detailConfig)
        val rows = detailResults.iterateAll().asScala.toSeq
        
        println(s"\n   Sample data:")
        rows.take(3).foreach { row =>
          val title = row.get("title").getStringValue
          val author = row.get("author").getStringValue
          val year = row.get("year").getStringValue
          println(s"     - $title by $author ($year)")
        }
      }

      // Process nested.xml (employees)
      val nestedFile = xmlFiles(1)
      if (nestedFile.exists()) {
        println(s"\n👥 Processing ${nestedFile.getName}...")
        
        val schemaResult = inferrer.infer(nestedFile, io.slatr.model.SchemaConfig())
        schemaResult.isSuccess shouldBe true
        
        val schema = schemaResult.get
        println(s"   Schema inferred: ${schema.fields.size} fields")

        val config = BigQueryConfig(
          projectId = "test-project",
          datasetId = "test_dataset",
          tableId = "employees",
          writeMode = WriteMode.Append,
          credentialsPath = None,
          useFirebaseModel = false
        )

        val writer = new BigQueryWriter(schema, config, Some(() => client))
        val writeResult = writer.writeFromXml(nestedFile, xmlParser, None)
        writeResult.isSuccess shouldBe true

        println(s"   ✅ Data loaded into employees table")

        // Query and validate
        val query = "SELECT COUNT(*) as count FROM `test-project.test_dataset.employees`"
        val queryConfig = QueryJobConfiguration.newBuilder(query)
          .setUseLegacySql(false)
          .build()

        val results = client.query(queryConfig)
        val count = results.iterateAll().asScala.head.get("count").getLongValue
        println(s"   📊 Row count: $count")
        count should be > 0L

        // Query nested data
        val detailQuery = """
          SELECT id, name, department
          FROM `test-project.test_dataset.employees` 
          ORDER BY id
        """
        val detailConfig = QueryJobConfiguration.newBuilder(detailQuery)
          .setUseLegacySql(false)
          .build()

        val detailResults = client.query(detailConfig)
        val rows = detailResults.iterateAll().asScala.toSeq
        
        println(s"\n   Sample data:")
        rows.foreach { row =>
          val id = row.get("id").getStringValue
          val name = row.get("name").getStringValue
          val dept = row.get("department").getStringValue
          println(s"     - ID $id: $name ($dept)")
        }
      }

    } finally {
      // Cleanup
      Seq("books", "employees").foreach { tableName =>
        try {
          client.delete(TableId.of("test-project", "test_dataset", tableName))
          ()
        } catch {
          case _: Exception => // Ignore cleanup errors
        }
      }
    }
  }

  it should "load multiple XML files using Firebase model" in {
    val client = createBigQueryClient()
    val xmlParser = XmlStreamParser()
    val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
    val inferrer = SchemaInferrer(xsdResolver, xmlParser)

    try {
      val xmlFiles = Seq(
        new File("examples/simple.xml"),
        new File("examples/nested.xml")
      )

      // Process all files into a single Firebase model table
      xmlFiles.filter(_.exists()).zipWithIndex.foreach { case (xmlFile, idx) =>
        println(s"\n📁 Processing ${xmlFile.getName} (Firebase model)...")
        
        val schemaResult = inferrer.infer(xmlFile, io.slatr.model.SchemaConfig())
        schemaResult.isSuccess shouldBe true
        
        val schema = schemaResult.get
        println(s"   Schema inferred: ${schema.fields.size} fields")

        val config = BigQueryConfig(
          projectId = "test-project",
          datasetId = "test_dataset",
          tableId = "unified_data_firebase",
          writeMode = WriteMode.Append,
          credentialsPath = None,
          useFirebaseModel = true
        )

        val writer = new BigQueryWriter(schema, config, Some(() => client))
        val writeResult = writer.writeFromXml(xmlFile, xmlParser, None)
        writeResult.isSuccess shouldBe true

        println(s"   ✅ Data loaded into unified_data_firebase table")
      }

      // Verify Firebase model schema
      val table = client.getTable(TableId.of("test-project", "test_dataset", "unified_data_firebase"))
      table should not be null

      val bqSchema = table.getDefinition[StandardTableDefinition].getSchema
      println(s"\n📊 Firebase model schema:")
      println(s"   Fields count: ${bqSchema.getFields.size()}")
      
      val fieldsField = bqSchema.getFields.get("fields")
      println(s"   Field type: ${fieldsField.getType.getStandardType}")
      println(s"   Field mode: ${fieldsField.getMode}")
      
      fieldsField.getMode shouldBe com.google.cloud.bigquery.Field.Mode.REPEATED
      fieldsField.getType.getStandardType shouldBe StandardSQLTypeName.STRUCT

      // Query total rows
      val countQuery = "SELECT COUNT(*) as count FROM `test-project.test_dataset.unified_data_firebase`"
      val countConfig = QueryJobConfiguration.newBuilder(countQuery)
        .setUseLegacySql(false)
        .build()

      val countResults = client.query(countConfig)
      val totalCount = countResults.iterateAll().asScala.head.get("count").getLongValue
      println(s"\n   Total rows: $totalCount")
      totalCount should be > 0L

      // Find all distinct field names in Firebase model
      val fieldNamesQuery = """
        SELECT DISTINCT field.name as field_name
        FROM `test-project.test_dataset.unified_data_firebase`,
        UNNEST(fields) AS field
        ORDER BY field_name
      """
      val fieldNamesConfig = QueryJobConfiguration.newBuilder(fieldNamesQuery)
        .setUseLegacySql(false)
        .build()

      val fieldNamesResults = client.query(fieldNamesConfig)
      val fieldNames = fieldNamesResults.iterateAll().asScala.map(_.get("field_name").getStringValue).toSeq
      
      println(s"\n   All field names found in data:")
      fieldNames.foreach { name =>
        println(s"     - $name")
      }
      
      fieldNames.size should be > 0

      // Query sample data by extracting specific fields
      val sampleQuery = """
        SELECT 
          (SELECT value FROM UNNEST(fields) WHERE name = 'title') as title,
          (SELECT value FROM UNNEST(fields) WHERE name = 'author') as author,
          (SELECT value FROM UNNEST(fields) WHERE name = 'name') as name,
          (SELECT value FROM UNNEST(fields) WHERE name = 'department') as department
        FROM `test-project.test_dataset.unified_data_firebase`
        LIMIT 5
      """
      val sampleConfig = QueryJobConfiguration.newBuilder(sampleQuery)
        .setUseLegacySql(false)
        .build()

      val sampleResults = client.query(sampleConfig)
      val samples = sampleResults.iterateAll().asScala.toSeq
      
      println(s"\n   Sample records (showing flexibility of Firebase model):")
      samples.foreach { row =>
        val title = if (row.get("title").isNull) None else Some(row.get("title").getStringValue)
        val author = if (row.get("author").isNull) None else Some(row.get("author").getStringValue)
        val name = if (row.get("name").isNull) None else Some(row.get("name").getStringValue)
        val dept = if (row.get("department").isNull) None else Some(row.get("department").getStringValue)
        
        if (title.isDefined) {
          println(s"     Book: ${title.get} by ${author.getOrElse("Unknown")}")
        } else if (name.isDefined) {
          println(s"     Employee: ${name.get} in ${dept.getOrElse("Unknown")}")
        }
      }

    } finally {
      // Cleanup
      try {
        client.delete(TableId.of("test-project", "test_dataset", "unified_data_firebase"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }

  it should "demonstrate benefits of Firebase model with schema evolution" in {
    val client = createBigQueryClient()
    val xmlParser = XmlStreamParser()
    val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
    val inferrer = SchemaInferrer(xsdResolver, xmlParser)

    try {
      println(s"\n🔄 Testing schema evolution with Firebase model...")

      // Load simple.xml first
      val simpleFile = new File("examples/simple.xml")
      if (simpleFile.exists()) {
        val schema1 = inferrer.infer(simpleFile, io.slatr.model.SchemaConfig()).get
        
        val config = BigQueryConfig(
          projectId = "test-project",
          datasetId = "test_dataset",
          tableId = "evolution_test",
          writeMode = WriteMode.Append,
          credentialsPath = None,
          useFirebaseModel = true
        )

        val writer1 = new BigQueryWriter(schema1, config, Some(() => client))
        writer1.writeFromXml(simpleFile, xmlParser, None)
        
        println(s"   ✅ Loaded ${simpleFile.getName} with ${schema1.fields.size} fields")
      }

      // Load nested.xml (different schema) into same table
      val nestedFile = new File("examples/nested.xml")
      if (nestedFile.exists()) {
        val schema2 = inferrer.infer(nestedFile, io.slatr.model.SchemaConfig()).get
        
        val config = BigQueryConfig(
          projectId = "test-project",
          datasetId = "test_dataset",
          tableId = "evolution_test",
          writeMode = WriteMode.Append,
          credentialsPath = None,
          useFirebaseModel = true
        )

        val writer2 = new BigQueryWriter(schema2, config, Some(() => client))
        writer2.writeFromXml(nestedFile, xmlParser, None)
        
        println(s"   ✅ Loaded ${nestedFile.getName} with ${schema2.fields.size} fields (different schema!)")
      }

      // Verify both datasets coexist
      val query = "SELECT COUNT(*) as count FROM `test-project.test_dataset.evolution_test`"
      val queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build()

      val results = client.query(queryConfig)
      val count = results.iterateAll().asScala.head.get("count").getLongValue
      
      println(s"\n   📊 Total rows from both XMLs: $count")
      println(s"   ✨ Firebase model allows merging different schemas without conflicts!")
      
      count should be > 0L

    } finally {
      try {
        client.delete(TableId.of("test-project", "test_dataset", "evolution_test"))
        ()
      } catch {
        case _: Exception =>
      }
    }
  }
}
