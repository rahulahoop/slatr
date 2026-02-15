package io.slatr.tools

import io.slatr.converter.PostgreSQLWriter
import io.slatr.model.{PostgreSQLConfig, SchemaConfig, WriteMode, XsdConfig}
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}

import java.io.File

/**
 * Standalone utility to load an XML file into a local PostgreSQL instance.
 *
 * Configuration is read from system properties (set via sbt -D flags):
 *   slatr.pg.host, slatr.pg.port, slatr.pg.database,
 *   slatr.pg.user, slatr.pg.password, slatr.pg.table, slatr.pg.xmlFile
 *
 * Usage:
 *   sbt -Dslatr.pg.xmlFile=examples/out.xml "core/runMain io.slatr.tools.LoadToPostgres"
 */
object LoadToPostgres {

  def main(args: Array[String]): Unit = {
    val host     = sys.props.getOrElse("slatr.pg.host", "localhost")
    val port     = sys.props.getOrElse("slatr.pg.port", "5432").toInt
    val database = sys.props.getOrElse("slatr.pg.database", "music_metadata")
    val user     = sys.props.getOrElse("slatr.pg.user", "slatr")
    val password = sys.props.getOrElse("slatr.pg.password", "slatr")
    val table    = sys.props.getOrElse("slatr.pg.table", "ddex_releases")
    val xmlPath  = sys.props.getOrElse("slatr.pg.xmlFile", "examples/out.xml")

    val xmlFile = new File(xmlPath)
    require(xmlFile.exists(), s"XML file not found: ${xmlFile.getAbsolutePath}")

    println(s"Loading ${xmlFile.getName} into PostgreSQL")
    println(s"  Host:     $host:$port")
    println(s"  Database: $database")
    println(s"  Table:    $table")
    println()

    val xmlParser  = XmlStreamParser()
    val xsdResolver = XsdResolver(XsdConfig())
    val inferrer   = SchemaInferrer(xsdResolver, xmlParser)

    println("Inferring schema...")
    val schema = inferrer.infer(xmlFile, SchemaConfig()).getOrElse {
      System.err.println("Schema inference failed")
      sys.exit(1)
      throw new RuntimeException // unreachable
    }
    println(s"  Fields: ${schema.fields.size}")

    val config = PostgreSQLConfig(
      host          = host,
      port          = port,
      database      = database,
      schema        = "public",
      table         = table,
      username      = user,
      password      = password,
      writeMode     = WriteMode.Append,
      useFirebaseModel = true
    )

    val writer = PostgreSQLWriter(schema, config)
    writer.writeFromXml(xmlFile, xmlParser) match {
      case scala.util.Success(tbl) =>
        println(s"  Loaded into $tbl")
      case scala.util.Failure(e) =>
        System.err.println(s"Write failed: ${e.getMessage}")
        sys.exit(1)
    }

    println()
    println("Done. Query with:")
    println(s"  docker exec -it slatr-postgres psql -U $user -d $database -c \"SELECT * FROM ddex_releases_flat;\"")
  }
}
