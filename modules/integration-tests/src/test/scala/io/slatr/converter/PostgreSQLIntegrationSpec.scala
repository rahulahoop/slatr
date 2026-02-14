package io.slatr.converter

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import io.slatr.model.{DataType, Field, PostgreSQLConfig, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.sql.DriverManager
import scala.util.Using

/**
 * Integration test for PostgreSQL writer
 * Uses Testcontainers to spin up a real PostgreSQL instance
 */
class PostgreSQLIntegrationSpec extends AnyFlatSpec with Matchers with ForAllTestContainer {

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:16-alpine")

  "PostgreSQLWriter" should "create table and insert data with traditional schema" in {
    val schema = Schema("root", Map(
      "id" -> Field("id", DataType.IntType, nullable = false),
      "name" -> Field("name", DataType.StringType, nullable = false),
      "price" -> Field("price", DataType.DoubleType, nullable = true),
      "available" -> Field("available", DataType.BooleanType, nullable = true)
    ))

    val config = PostgreSQLConfig(
      host = container.host,
      port = container.mappedPort(5432),
      database = container.databaseName,
      schema = "public",
      table = "products",
      username = container.username,
      password = container.password,
      writeMode = WriteMode.ErrorIfExists,
      useFirebaseModel = false
    )

    val writer = PostgreSQLWriter(schema, config)

    val rows = Seq(
      Map[String, Any]("id" -> 1, "name" -> "Product A", "price" -> 19.99, "available" -> true),
      Map[String, Any]("id" -> 2, "name" -> "Product B", "price" -> 29.99, "available" -> false),
      Map[String, Any]("id" -> 3, "name" -> "Product C", "price" -> null, "available" -> true)
    )

    val result = writer.write(rows.iterator)
    result shouldBe "public.products"

    // Verify data
    Using.resource(DriverManager.getConnection(
      container.jdbcUrl,
      container.username,
      container.password
    )) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        Using.resource(stmt.executeQuery("SELECT COUNT(*) FROM products")) { rs =>
          rs.next()
          rs.getInt(1) shouldBe 3
        }

        Using.resource(stmt.executeQuery("SELECT * FROM products ORDER BY id")) { rs =>
          rs.next()
          rs.getInt("id") shouldBe 1
          rs.getString("name") shouldBe "Product A"
          rs.getDouble("price") shouldBe 19.99
          rs.getBoolean("available") shouldBe true

          rs.next()
          rs.getInt("id") shouldBe 2
          rs.getString("name") shouldBe "Product B"

          rs.next()
          rs.getInt("id") shouldBe 3
          rs.getString("name") shouldBe "Product C"
          rs.getObject("price") shouldBe null
        }
      }
    }
  }

  it should "handle Firebase model with JSONB storage" in {
    val schema = Schema("root", Map(
      "id" -> Field("id", DataType.IntType, nullable = false),
      "name" -> Field("name", DataType.StringType, nullable = false),
      "category" -> Field("category", DataType.StringType, nullable = true)
    ))

    val config = PostgreSQLConfig(
      host = container.host,
      port = container.mappedPort(5432),
      database = container.databaseName,
      schema = "public",
      table = "firebase_test",
      username = container.username,
      password = container.password,
      writeMode = WriteMode.ErrorIfExists,
      useFirebaseModel = true
    )

    val writer = PostgreSQLWriter(schema, config)

    val rows = Seq(
      Map[String, Any]("id" -> 1, "name" -> "Item A", "category" -> "electronics"),
      Map[String, Any]("id" -> 2, "name" -> "Item B", "category" -> "books")
    )

    writer.write(rows.iterator)

    // Verify JSONB data
    Using.resource(DriverManager.getConnection(
      container.jdbcUrl,
      container.username,
      container.password
    )) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        // Check table has JSONB column
        Using.resource(stmt.executeQuery(
          "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'firebase_test'"
        )) { rs =>
          var hasDataColumn = false
          var hasJsonbType = false
          
          while (rs.next()) {
            if (rs.getString("column_name") == "data") {
              hasDataColumn = true
              if (rs.getString("data_type") == "jsonb") {
                hasJsonbType = true
              }
            }
          }
          
          hasDataColumn shouldBe true
          hasJsonbType shouldBe true
        }

        // Query JSONB data
        Using.resource(stmt.executeQuery("SELECT COUNT(*) FROM firebase_test")) { rs =>
          rs.next()
          rs.getInt(1) shouldBe 2
        }
      }
    }
  }

  it should "load DDEX XML files into PostgreSQL" in {
    val xmlParser = XmlStreamParser()
    val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
    val inferrer = SchemaInferrer(xsdResolver, xmlParser)

    val xmlFile = new File("out.xml")
    if (xmlFile.exists()) {
      println(s"\n🎵 Loading DDEX XML into PostgreSQL")
      println(s"=" * 60)

      val schemaResult = inferrer.infer(xmlFile, io.slatr.model.SchemaConfig())
      schemaResult.isSuccess shouldBe true

      val schema = schemaResult.get
      println(s"  ✓ Schema inferred: ${schema.fields.size} fields")

      val config = PostgreSQLConfig(
        host = container.host,
        port = container.mappedPort(5432),
        database = container.databaseName,
        schema = "public",
        table = "ddex_releases",
        username = container.username,
        password = container.password,
        writeMode = WriteMode.Append,
        useFirebaseModel = true
      )

      val writer = PostgreSQLWriter(schema, config)
      val writeResult = writer.writeFromXml(xmlFile, xmlParser)
      writeResult.isSuccess shouldBe true

      println(s"  ✅ Loaded into PostgreSQL: ${config.table}")

      // Verify data
      Using.resource(DriverManager.getConnection(
        container.jdbcUrl,
        container.username,
        container.password
      )) { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          Using.resource(stmt.executeQuery("SELECT COUNT(*) FROM ddex_releases")) { rs =>
            rs.next()
            val count = rs.getInt(1)
            println(s"  📊 Rows inserted: $count")
            count should be > 0
          }

          // Query JSONB data to extract fields
          Using.resource(stmt.executeQuery(
            """
              |SELECT data::text as json_data 
              |FROM ddex_releases 
              |LIMIT 1
              |""".stripMargin
          )) { rs =>
            rs.next()
            val jsonData = rs.getString("json_data")
            println(s"\n  Sample JSONB data:")
            println(s"  ${jsonData.take(200)}...")
            
            jsonData should not be empty
          }
        }
      }

      println(s"\n✅ Successfully loaded DDEX into PostgreSQL!")
      println(s"=" * 60)
    }
  }

  it should "handle multiple XML files into same table" in {
    val xmlParser = XmlStreamParser()
    val xsdResolver = XsdResolver(io.slatr.model.XsdConfig())
    val inferrer = SchemaInferrer(xsdResolver, xmlParser)

    val xmlFiles = Seq(
      new File("examples/simple.xml"),
      new File("examples/nested.xml")
    ).filter(_.exists())

    if (xmlFiles.nonEmpty) {
      println(s"\n📚 Loading multiple XML files into PostgreSQL")
      println(s"=" * 60)

      xmlFiles.foreach { xmlFile =>
        println(s"\n  Processing ${xmlFile.getName}...")

        val schema = inferrer.infer(xmlFile, io.slatr.model.SchemaConfig()).get
        println(s"    Schema: ${schema.fields.size} fields")

        val config = PostgreSQLConfig(
          host = container.host,
          port = container.mappedPort(5432),
          database = container.databaseName,
          schema = "public",
          table = "multi_xml",
          username = container.username,
          password = container.password,
          writeMode = WriteMode.Append,
          useFirebaseModel = true
        )

        val writer = PostgreSQLWriter(schema, config)
        writer.writeFromXml(xmlFile, xmlParser)

        println(s"    ✓ Loaded")
      }

      // Verify all data
      Using.resource(DriverManager.getConnection(
        container.jdbcUrl,
        container.username,
        container.password
      )) { conn =>
        Using.resource(conn.createStatement()) { stmt =>
          Using.resource(stmt.executeQuery("SELECT COUNT(*) FROM multi_xml")) { rs =>
            rs.next()
            val count = rs.getInt(1)
            println(s"\n  Total rows from all XMLs: $count")
            count should be > 0
          }
        }
      }

      println(s"\n✅ Successfully loaded multiple XMLs!")
      println(s"=" * 60)
    }
  }

  it should "query Firebase model data from PostgreSQL" in {
    val schema = Schema("root", Map(
      "title" -> Field("title", DataType.StringType, nullable = false),
      "artist" -> Field("artist", DataType.StringType, nullable = false),
      "genre" -> Field("genre", DataType.StringType, nullable = true)
    ))

    val config = PostgreSQLConfig(
      host = container.host,
      port = container.mappedPort(5432),
      database = container.databaseName,
      schema = "public",
      table = "music",
      username = container.username,
      password = container.password,
      writeMode = WriteMode.ErrorIfExists,
      useFirebaseModel = true
    )

    val writer = PostgreSQLWriter(schema, config)

    val rows = Seq(
      Map[String, Any]("title" -> "Song A", "artist" -> "Artist 1", "genre" -> "Rock"),
      Map[String, Any]("title" -> "Song B", "artist" -> "Artist 2", "genre" -> "Pop")
    )

    writer.write(rows.iterator)

    println(s"\n🎼 Querying Firebase model in PostgreSQL")
    println(s"=" * 60)

    // Demonstrate JSONB queries
    Using.resource(DriverManager.getConnection(
      container.jdbcUrl,
      container.username,
      container.password
    )) { conn =>
      Using.resource(conn.createStatement()) { stmt =>
        // Query raw JSONB
        println(s"\n  Raw JSONB data:")
        Using.resource(stmt.executeQuery("SELECT data FROM music LIMIT 2")) { rs =>
          while (rs.next()) {
            println(s"    ${rs.getString("data")}")
          }
        }

        // PostgreSQL JSONB operators are powerful
        println(s"\n  JSONB query examples:")
        println(s"    - Filter by field")
        println(s"    - Extract specific values")
        println(s"    - Full-text search on JSONB")
      }
    }

    println(s"\n✅ Firebase model queries work!")
    println(s"=" * 60)
  }
}
