package io.slatr.tools

import io.slatr.converter.PostgreSQLWriter
import io.slatr.model.{PostgreSQLConfig, SchemaConfig, WriteMode, XsdConfig}
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.{SchemaInferrer, XsdResolver}

import java.io.File

/**
 * Standalone utility to load XML files into a local PostgreSQL instance.
 *
 * Supports loading a single file or every .xml in a directory.
 * Toggle between Firebase JSONB model and traditional columnar model.
 *
 * System properties (set via sbt -D flags):
 *   slatr.pg.host, slatr.pg.port, slatr.pg.database,
 *   slatr.pg.user, slatr.pg.password,
 *   slatr.pg.table    -- target table name override
 *   slatr.pg.xmlFile  -- path to a single XML file
 *   slatr.pg.xmlDir   -- path to a directory; loads every *.xml inside
 *                        (default: "examples")
 *   slatr.pg.mode     -- "firebase" (default) or "traditional"
 *
 * If xmlFile is set, only that file is loaded. Otherwise xmlDir is used
 * (defaults to "examples"). In firebase mode all files go into a single
 * "ddex_releases" table. In traditional mode each file gets its own table
 * named after the file (sanitised). Setting slatr.pg.table overrides the
 * table name in either mode.
 *
 * Examples:
 *   # All examples XML files, firebase model (default)
 *   sbt "core/runMain io.slatr.tools.LoadToPostgres"
 *
 *   # Single file
 *   sbt -Dslatr.pg.xmlFile=examples/43_Audio.xml \
 *       "core/runMain io.slatr.tools.LoadToPostgres"
 *
 *   # All examples, traditional columns
 *   sbt -Dslatr.pg.mode=traditional \
 *       "core/runMain io.slatr.tools.LoadToPostgres"
 *
 *   # Custom directory
 *   sbt -Dslatr.pg.xmlDir=/tmp/my-xml \
 *       "core/runMain io.slatr.tools.LoadToPostgres"
 */
object LoadToPostgres {

  def main(args: Array[String]): Unit = {
    val host     = sys.props.getOrElse("slatr.pg.host", "localhost")
    val port     = sys.props.getOrElse("slatr.pg.port", "5432").toInt
    val database = sys.props.getOrElse("slatr.pg.database", "music_metadata")
    val user     = sys.props.getOrElse("slatr.pg.user", "slatr")
    val password = sys.props.getOrElse("slatr.pg.password", "slatr")
    val tableOpt = sys.props.get("slatr.pg.table")
    val xmlFile  = sys.props.get("slatr.pg.xmlFile")
    val xmlDir   = sys.props.getOrElse("slatr.pg.xmlDir", "examples")
    val mode     = sys.props.getOrElse("slatr.pg.mode", "firebase")

    val useFirebase = mode.toLowerCase match {
      case "firebase" | "jsonb" | "fb" => true
      case "traditional" | "columnar" | "columns" | "trad" => false
      case other =>
        System.err.println(s"Unknown mode: $other (use 'firebase' or 'traditional')")
        sys.exit(1)
        false // unreachable
    }

    val modeLabel = if (useFirebase) "firebase (JSONB)" else "traditional (columns)"

    // Resolve list of XML files to load.
    // If xmlFile is set, load only that file; otherwise load every *.xml in xmlDir.
    val xmlFiles: Seq[File] = xmlFile match {
      case Some(path) =>
        val f = new File(path)
        require(f.exists(), s"XML file not found: ${f.getAbsolutePath}")
        Seq(f)
      case None =>
        val d = new File(xmlDir)
        require(d.isDirectory, s"Not a directory: ${d.getAbsolutePath}")
        d.listFiles().filter(f => f.isFile && f.getName.endsWith(".xml")).sorted.toSeq
    }

    require(xmlFiles.nonEmpty, "No XML files found")

    println(s"PostgreSQL XML Loader")
    println(s"=====================")
    println(s"  Host:     $host:$port")
    println(s"  Database: $database")
    println(s"  Mode:     $modeLabel")
    println(s"  Files:    ${xmlFiles.size}")
    println()

    val xmlParser   = XmlStreamParser()
    val xsdResolver = XsdResolver(XsdConfig())
    val inferrer    = SchemaInferrer(xsdResolver, xmlParser)

    var loaded  = 0
    var failed  = 0

    xmlFiles.foreach { file =>
      val tableName = tableOpt.getOrElse(
        if (useFirebase) "ddex_releases" else tableNameFromFile(file)
      )
      print(s"  ${file.getName} -> $tableName ... ")

      try {
        val schema = inferrer.infer(file, SchemaConfig()).getOrElse {
          throw new RuntimeException("schema inference failed")
        }

        val config = PostgreSQLConfig(
          host             = host,
          port             = port,
          database         = database,
          schema           = "public",
          table            = tableName,
          username         = user,
          password         = password,
          writeMode        = WriteMode.Append,
          useFirebaseModel = useFirebase
        )

        val writer = PostgreSQLWriter(schema, config)
        writer.writeFromXml(file, xmlParser) match {
          case scala.util.Success(_) =>
            println(s"OK (${schema.fields.size} fields)")
            loaded += 1
          case scala.util.Failure(e) =>
            println(s"WRITE FAILED: ${e.getMessage}")
            failed += 1
        }
      } catch {
        case e: Exception =>
          println(s"FAILED: ${e.getMessage}")
          failed += 1
      }
    }

    println()
    println(s"Done. Loaded $loaded/${xmlFiles.size} files" +
      (if (failed > 0) s" ($failed failed)" else ""))
    println()
    println("Query with:")
    println(s"  just pg-psql")
    println(s"  just pg-status")
  }

  /** Derive a clean PostgreSQL table name from a filename. */
  private def tableNameFromFile(f: File): String = {
    val raw = f.getName
      .replaceAll("\\.xml$", "")
      .replaceAll("[^a-zA-Z0-9_]", "_")
      .replaceAll("_+", "_")
      .replaceAll("^_|_$", "")
      .toLowerCase

    // PostgreSQL identifiers cannot start with a digit
    if (raw.headOption.exists(_.isDigit)) s"ddex_$raw" else raw
  }
}
