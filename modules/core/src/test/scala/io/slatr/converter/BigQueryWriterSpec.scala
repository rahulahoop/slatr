package io.slatr.converter

import com.google.cloud.bigquery.{BigQuery, BigQueryError, InsertAllRequest, InsertAllResponse}
import io.slatr.model.{BigQueryConfig, DataType, Field, Schema, WriteMode}
import io.slatr.parser.XmlStreamParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

/**
 * Tests the real `write()` path with the Firebase model, capturing the
 * InsertAllRequest sent to BigQuery so we can assert on what actually gets stored.
 *
 * BigQuery is faked with a java.lang.reflect.Proxy rather than a mocking library
 * because the Google client's getTable/create are overloaded varargs methods and
 * Table has an inaccessible constructor — both awkward for ScalaMock/Mockito.
 */
class BigQueryWriterSpec extends AnyFlatSpec with Matchers {

  /** Build an InsertAllResponse with no errors (package-private ctor → reflection). */
  private def emptyResponse(): InsertAllResponse = {
    val ctor = classOf[InsertAllResponse].getDeclaredConstructor(classOf[java.util.Map[_, _]])
    ctor.setAccessible(true)
    ctor.newInstance(new java.util.HashMap[java.lang.Long, java.util.List[BigQueryError]]())
  }

  /**
   * Fake BigQuery via Proxy. getTable returns null (→ create path), create returns
   * null (return value is ignored by write), insertAll captures the request.
   */
  private def fakeBigQuery(captured: ListBuffer[InsertAllRequest]): BigQuery = {
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
        method.getName match {
          case "insertAll" =>
            captured += args(0).asInstanceOf[InsertAllRequest]
            emptyResponse()
          case "equals"   => java.lang.Boolean.valueOf(proxy.asInstanceOf[AnyRef] eq args(0))
          case "hashCode" => Integer.valueOf(System.identityHashCode(proxy))
          case "toString" => "FakeBigQuery"
          case _          => null // getTable, create, etc.
        }
    }
    Proxy
      .newProxyInstance(getClass.getClassLoader, Array(classOf[BigQuery]), handler)
      .asInstanceOf[BigQuery]
  }

  /** Run write() against the fake and return the captured field name/value pairs. */
  private def captureFields(rows: Iterator[Map[String, Any]]): List[(String, Any)] = {
    val captured = ListBuffer[InsertAllRequest]()
    val config = BigQueryConfig(
      projectId = "p",
      datasetId = "d",
      tableId = "t",
      writeMode = WriteMode.Append,
      useFirebaseModel = true
    )
    val writer = new BigQueryWriter(Schema.empty("root"), config, Some(() => fakeBigQuery(captured)))
    writer.write(rows)

    captured.toList.flatMap { req =>
      req.getRows.asScala.flatMap { row =>
        val fields = row.getContent.get("fields").asInstanceOf[java.util.List[Any]]
        fields.asScala.toList.map { f =>
          val m = f.asInstanceOf[java.util.Map[String, Any]]
          m.get("name").toString -> m.get("value")
        }
      }
    }
  }

  "BigQueryWriter.write (Firebase model)" should "write all elements in an array, not just the first" in {
    val element = Map[String, Any](
      "SoundRecording" -> List(
        Map[String, Any]("ISRC" -> List(Map[String, Any]("#text" -> "USRC17607839"))),
        Map[String, Any]("ISRC" -> List(Map[String, Any]("#text" -> "USRC17607840"))),
        Map[String, Any]("ISRC" -> List(Map[String, Any]("#text" -> "USRC17607841")))
      )
    )

    val names = captureFields(Iterator(element)).map(_._1)

    // Parser wraps every child element in a List, so single occurrences are indexed too
    names should contain("SoundRecording[0].ISRC[0]")
    names should contain("SoundRecording[1].ISRC[0]")
    names should contain("SoundRecording[2].ISRC[0]")
  }

  it should "preserve correct values for each array element" in {
    val element = Map[String, Any](
      "SoundRecording" -> List(
        Map[String, Any]("ISRC" -> List(Map[String, Any]("#text" -> "USRC17607839"))),
        Map[String, Any]("ISRC" -> List(Map[String, Any]("#text" -> "USRC17607840")))
      )
    )

    val byName = captureFields(Iterator(element)).toMap

    byName("SoundRecording[0].ISRC[0]") shouldBe "USRC17607839"
    byName("SoundRecording[1].ISRC[0]") shouldBe "USRC17607840"
  }

  it should "not drop any elements from a large list (regression: 21 SoundRecordings)" in {
    val items   = (0 until 21).map(i => Map[String, Any]("#text" -> s"item$i")).toList
    val element = Map[String, Any]("SoundRecording" -> items)

    val names = captureFields(Iterator(element)).map(_._1)

    (0 until 21).foreach { i =>
      names should contain(s"SoundRecording[$i]")
    }
  }

  it should "preserve scalar values" in {
    val byName = captureFields(Iterator(Map[String, Any]("MessageId" -> "Test1.1"))).toMap
    byName("MessageId") shouldBe "Test1.1"
  }

  it should "extract correlation metadata and keep all resources from a real ERN file" in {
    val file = new File("examples/42_Audio.xml")
    assume(file.exists(), s"sample file not found: ${file.getAbsolutePath}")

    val captured = ListBuffer[InsertAllRequest]()
    val config = BigQueryConfig(
      projectId = "p",
      datasetId = "d",
      tableId = "t",
      writeMode = WriteMode.Append,
      useFirebaseModel = true
    )
    val writer = new BigQueryWriter(Schema.empty("root"), config, Some(() => fakeBigQuery(captured)))
    writer.writeFromXml(file, new XmlStreamParser()).get

    val rows  = captured.toList.flatMap(_.getRows.asScala)
    val metas = rows.map(_.getContent.asScala.toMap)

    // Per-file correlation columns from the MessageHeader, applied to every row
    all(metas.map(_.get("ern_version"))) shouldBe Some("4.2")
    metas.flatMap(_.get("message_id")).distinct should contain("Test1.1")
    metas.flatMap(_.get("message_sender_id")).distinct should contain("PADPIDA2013042401U")
    metas.flatMap(_.get("message_created_datetime")).distinct should
      contain("2014-09-24T14:57:25+01:00")
    metas.flatMap(_.get("element_name")).distinct should contain("ResourceList")
    all(metas.map(_.contains("ingested_at"))) shouldBe true

    // All 21 SoundRecordings preserved (regression for the array-drop bug)
    val fieldNames = rows.flatMap { row =>
      row.getContent.get("fields").asInstanceOf[java.util.List[Any]].asScala.map { f =>
        f.asInstanceOf[java.util.Map[String, Any]].get("name").toString
      }
    }
    (0 until 21).foreach { i =>
      withClue(s"missing SoundRecording[$i]: ") {
        fieldNames.exists(_.startsWith(s"SoundRecording[$i]")) shouldBe true
      }
    }
  }

  /** Run a traditional (non-Firebase) write() and return the captured row content maps. */
  private def captureTraditional(
    schema: Schema,
    rows: Iterator[Map[String, Any]]
  ): List[Map[String, Any]] = {
    val captured = ListBuffer[InsertAllRequest]()
    val config = BigQueryConfig(
      projectId = "p",
      datasetId = "d",
      tableId = "t",
      writeMode = WriteMode.Append,
      useFirebaseModel = false
    )
    val writer = new BigQueryWriter(schema, config, Some(() => fakeBigQuery(captured)))
    writer.write(rows)
    captured.toList.flatMap(_.getRows.asScala.map(_.getContent.asScala.toMap))
  }

  "BigQueryWriter.write (traditional model)" should "flatten a depth-2 struct schema into scalar columns" in {
    // Inference yields {book: Struct{...}}; rows are the book's contents (parser shape).
    val schema = Schema(
      "catalog",
      Map(
        "book" -> Field(
          "book",
          DataType.StructType(
            Map(
              "title" -> Field("title", DataType.StringType, nullable = true, isArray = false),
              "year"  -> Field("year", DataType.IntType, nullable = true, isArray = false)
            )
          ),
          nullable = true,
          isArray = false
        )
      )
    )
    val row = Map[String, Any](
      "title" -> List(Map[String, Any]("#text" -> "The Great Gatsby")),
      "year"  -> List(Map[String, Any]("#text" -> "1925"))
    )

    val content = captureTraditional(schema, Iterator(row)).head
    content.get("title") shouldBe Some("The Great Gatsby")
    content.get("year") shouldBe Some(1925L) // IntType maps to BigQuery INT64 (Long)
  }

  it should "build a nested RECORD for struct columns" in {
    val schema = Schema(
      "company",
      Map(
        "employee" -> Field(
          "employee",
          DataType.StructType(
            Map(
              "id" -> Field("id", DataType.IntType, nullable = true, isArray = false),
              "contact" -> Field(
                "contact",
                DataType.StructType(
                  Map("email" -> Field("email", DataType.StringType, nullable = true, isArray = false))
                ),
                nullable = true,
                isArray = false
              )
            )
          ),
          nullable = true,
          isArray = false
        )
      )
    )
    val row = Map[String, Any](
      "id"      -> List(Map[String, Any]("#text" -> "1")),
      "contact" -> List(Map[String, Any]("email" -> List(Map[String, Any]("#text" -> "a@b.com"))))
    )

    val content = captureTraditional(schema, Iterator(row)).head
    content.get("id") shouldBe Some(1L)
    val contact = content("contact").asInstanceOf[java.util.Map[String, Any]]
    contact.get("email") shouldBe "a@b.com"
  }
}
