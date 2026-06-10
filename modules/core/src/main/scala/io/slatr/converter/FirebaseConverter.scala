package io.slatr.converter

import io.slatr.model.{Chunk, XmlElement}
import io.slatr.parser.XmlStreamParser

import java.io.File
import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * Converts parsed XML into BigQuery "Firebase model" content maps: a repeated `fields`
 * key/value struct of flattened leaf paths, plus top-level correlation columns.
 *
 * Each returned map is the BigQuery-serializable payload (a `java.util.Map[String, Object]`):
 * it is mutable (callers can add/drop/edit columns) and becomes a streaming row via
 * `com.google.cloud.bigquery.InsertAllRequest.RowToInsert.of(map)`.
 */
object FirebaseConverter {

  /** Top-level STRING correlation columns added to every Firebase-model row. */
  private[converter] val MetadataStringColumns: Seq[String] = Seq(
    "file_store_path",
    "element_name",
    "message_id",
    "message_sender_id",
    "message_created_datetime",
    "ern_version"
  )

  /** Pipeline ingestion timestamp column (TIMESTAMP). */
  private[converter] val IngestedAtColumn: String = "ingested_at"

  /**
   * Convert an XML file into one content map per depth-2 element. Every map carries the
   * message's correlation metadata (source path, ERN version, key MessageHeader fields) and
   * the element name, so the per-element rows can be reassembled downstream with
   * `GROUP BY message_id`. The maps are mutable and BigQuery-insertable.
   */
  def fromXml(
    xmlFile: File,
    xmlParser: XmlStreamParser = XmlStreamParser(),
    chunk: Option[Chunk] = None
  ): Seq[java.util.Map[String, AnyRef]] = {
    val elements = xmlParser
      .parseNamed(xmlFile, chunk)
      .getOrElse(throw new Exception("Failed to parse XML"))
      .toList

    val fileMetadata = buildFileMetadata(xmlFile, xmlParser, elements)

    // Root-element attributes belong to no depth-2 row, so carry them on every row's fields[]
    // as `@name` (document-level attributes such as ReleaseProfileVersionId).
    val rootAttributes = xmlParser.extractRootAttributes(xmlFile).toList.map { case (k, v) => s"@$k" -> v }

    elements.map { case (name, element) =>
      toMap(element, fileMetadata + ("element_name" -> name), rootAttributes)
    }
  }

  /**
   * Build a single Firebase content map: the flattened leaf paths in the repeated `fields`
   * struct, plus the top-level correlation columns from `metadata` and an `ingested_at` stamp.
   */
  private[converter] def toMap(
    element: XmlElement,
    metadata: Map[String, String],
    extraFields: List[(String, String)] = Nil
  ): java.util.Map[String, AnyRef] = {
    val fields: java.util.List[java.util.Map[String, String]] =
      (toFirebaseFields(element) ++ extraFields).map { case (name, value) =>
        Map("name" -> name, "value" -> value).asJava
      }.asJava

    val content = scala.collection.mutable.Map[String, AnyRef]("fields" -> fields)
    metadata.foreach { case (k, v) => content(k) = v }
    content(IngestedAtColumn) = Instant.now().toString
    content.asJava
  }

  /**
   * Flatten an element into Firebase leaf paths: `(path, value)` pairs. Fully recursive —
   * every child occurrence is indexed (`name[idx]`), nested children are dotted
   * (`parent[i].child[j]`), attributes are `@name`, and leaf text is the value.
   */
  private[converter] def toFirebaseFields(element: XmlElement): List[(String, String)] = {
    val textEntry   = element.text.map(t => "#text" -> t).toList
    val attrEntries = element.attributes.toList.map { case (k, v) => s"@$k" -> v }
    val childEntries = element.children.toList.flatMap { case (name, els) =>
      els.zipWithIndex.flatMap { case (child, idx) => flattenAt(s"$name[$idx]", child) }
    }
    textEntry ++ attrEntries ++ childEntries
  }

  /** Flatten an element nested at `path`, prefixing every produced key with `path`. */
  private def flattenAt(path: String, element: XmlElement): List[(String, String)] = {
    val textEntry   = element.text.map(t => path -> t).toList
    val attrEntries = element.attributes.toList.map { case (k, v) => s"$path.@$k" -> v }
    val childEntries = element.children.toList.flatMap { case (name, els) =>
      els.zipWithIndex.flatMap { case (child, idx) => flattenAt(s"$path.$name[$idx]", child) }
    }
    textEntry ++ attrEntries ++ childEntries
  }

  /**
   * Build per-file correlation metadata: source path, ERN version (from the namespace), and
   * key MessageHeader fields. Empty values are dropped.
   */
  private def buildFileMetadata(
    file: File,
    xmlParser: XmlStreamParser,
    elements: List[(String, XmlElement)]
  ): Map[String, String] = {
    val header = elements.collectFirst { case ("MessageHeader", m) => m }
    def hv(path: String*): Option[String] = header.flatMap(_.textAt(path: _*))

    Map(
      "file_store_path"          -> Some(file.getAbsolutePath),
      "ern_version"              -> xmlParser.extractErnVersion(file),
      "message_id"               -> hv("MessageId"),
      "message_sender_id"        -> hv("MessageSender", "PartyId"),
      "message_created_datetime" -> hv("MessageCreatedDateTime")
    ).collect { case (k, Some(v)) if v.nonEmpty => k -> v }
  }
}
