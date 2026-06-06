package io.slatr.parser

import com.fasterxml.aalto.stax.InputFactoryImpl
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{Chunk, XmlElement}

import java.io.{File, FileInputStream}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import scala.collection.mutable
import scala.util.{Try, Using}

/** Streaming XML parser using Aalto XML */
class XmlStreamParser extends LazyLogging {
  
  private val inputFactory: XMLInputFactory = new InputFactoryImpl()
  
  /**
   * Parse XML file and extract elements as a stream of [[XmlElement]]s.
   * Each element is a depth-2 child of the root element.
   */
  def parse(file: File, chunk: Option[Chunk] = None): Try[Iterator[XmlElement]] = {
    parseNamed(file, chunk).map(_.map(_._2))
  }

  /**
   * Parse XML file and return named elements: (elementName, element) pairs.
   * Each pair represents a depth-2 child of the root element.
   */
  def parseNamed(file: File, chunk: Option[Chunk] = None): Try[Iterator[(String, XmlElement)]] = {
    Try {
      val stream = new FileInputStream(file)
      val reader = inputFactory.createXMLStreamReader(stream)
      
      chunk.foreach { c =>
        if (c.startOffset > 0) {
          stream.skip(c.startOffset)
        }
      }
      
      new XmlElementIterator(reader, chunk.map(_.endOffset))
    }
  }
  
  /**
   * Extract XSD URL from XML header (checks root element attributes only)
   */
  def extractXsdUrl(file: File): Option[String] = {
    Using(new FileInputStream(file)) { stream =>
      val reader = inputFactory.createXMLStreamReader(stream)

      try {
        advanceToFirstStartElement(reader).flatMap { _ =>
          val attrs = (0 until reader.getAttributeCount).map { i =>
            reader.getAttributeLocalName(i) -> reader.getAttributeValue(i)
          }

          attrs.collectFirst { case ("schemaLocation", value) => value }
            .flatMap { value =>
              val urls = value.split("\\s+").filter(_.startsWith("http"))
              if (urls.length >= 2) Some(urls(1))
              else urls.headOption
            }
            .orElse {
              attrs.collectFirst { case ("noNamespaceSchemaLocation", value) if value.startsWith("http") =>
                value.trim
              }
            }
        }
      } finally {
        reader.close()
      }
    }.toOption.flatten
  }
  
  /**
   * Extract the ERN version from the root element's namespace declarations.
   * DDEX namespaces encode the version as a digit run, e.g.
   * `http://ddex.net/xml/ern/42` → "4.2", `.../ern/383` → "3.8.3".
   * Works across all ERN versions since the namespace is always present.
   */
  def extractErnVersion(file: File): Option[String] = {
    val ernNs = """ddex\.net/xml/ern/(\d+)""".r
    Using(new FileInputStream(file)) { stream =>
      val reader = inputFactory.createXMLStreamReader(stream)
      try {
        advanceToFirstStartElement(reader).flatMap { _ =>
          val uris =
            (0 until reader.getNamespaceCount).map(reader.getNamespaceURI) :+ reader.getNamespaceURI
          uris.iterator
            .flatMap(uri => Option(uri))
            .flatMap(uri => ernNs.findFirstMatchIn(uri).map(_.group(1)))
            .map(_.toSeq.mkString("."))
            .nextOption()
        }
      } finally {
        reader.close()
      }
    }.toOption.flatten
  }

  /**
   * Get root element name from XML file
   */
  def getRootElementName(file: File): Option[String] = {
    Using(new FileInputStream(file)) { stream =>
      val reader = inputFactory.createXMLStreamReader(stream)

      try {
        advanceToFirstStartElement(reader).map(_ => reader.getLocalName)
      } finally {
        reader.close()
      }
    }.toOption.flatten
  }

  /** Advance reader to the first START_ELEMENT, returning Some(()) if found, None if EOF. */
  @scala.annotation.tailrec
  private def advanceToFirstStartElement(reader: XMLStreamReader): Option[Unit] = {
    if (!reader.hasNext) None
    else if (reader.next() == XMLStreamConstants.START_ELEMENT) Some(())
    else advanceToFirstStartElement(reader)
  }
}

/**
 * Iterator that yields (elementName, element) pairs for depth-2 elements.
 */
private class XmlElementIterator(
  reader: XMLStreamReader,
  endOffset: Option[Long]
) extends Iterator[(String, XmlElement)] {

  private val stack = mutable.Stack[String]()
  private var current: Option[(String, XmlElement)] = None
  private var finished = false
  
  override def hasNext: Boolean = {
    if (finished) false
    else if (pastEndOffset) {
      finish()
      false
    }
    else if (current.isDefined) true
    else {
      try {
        var foundEnd = false
        while (reader.hasNext && current.isEmpty && !foundEnd) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT =>
              val elemName = reader.getLocalName
              stack.push(elemName)

              if (stack.size == 2) {
                current = Some((elemName, parseElement(reader)))
                stack.pop()
              }

            case XMLStreamConstants.END_ELEMENT =>
              if (stack.nonEmpty) stack.pop()

            case XMLStreamConstants.END_DOCUMENT =>
              foundEnd = true

            case _ =>
          }
        }

        if (current.isEmpty) finish()
        current.isDefined
      } catch {
        case e: Exception =>
          reader.close()
          throw e
      }
    }
  }

  private def pastEndOffset: Boolean = endOffset.exists { end =>
    reader.getLocation.getCharacterOffset >= end
  }

  private def finish(): Unit = {
    finished = true
    reader.close()
  }
  
  override def next(): (String, XmlElement) = {
    if (!hasNext) throw new NoSuchElementException("No more elements")
    val result = current.get
    current = None
    result
  }

  private def parseElement(reader: XMLStreamReader): XmlElement = {
    val attributes = (0 until reader.getAttributeCount).map { i =>
      reader.getAttributeLocalName(i) -> reader.getAttributeValue(i)
    }.toMap

    val childElements = mutable.Map[String, mutable.ArrayBuffer[XmlElement]]()
    var depth         = 1
    val textContent   = new StringBuilder()

    while (reader.hasNext && depth > 0) {
      reader.next() match {
        case XMLStreamConstants.START_ELEMENT =>
          depth += 1
          val childName = reader.getLocalName
          if (depth == 2) {
            val childValue = parseElement(reader)
            childElements.getOrElseUpdate(childName, mutable.ArrayBuffer()) += childValue
            depth -= 1
          }

        case XMLStreamConstants.END_ELEMENT =>
          depth -= 1

        case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
          val text = reader.getText
          if (text.trim.nonEmpty) textContent.append(text)

        case _ =>
      }
    }

    // Text is kept only for leaf elements, matching the previous representation.
    val text = if (childElements.isEmpty && textContent.nonEmpty) Some(textContent.toString.trim) else None
    val children = childElements.map { case (name, values) => name -> values.toList }.toMap

    XmlElement(attributes = attributes, text = text, children = children)
  }
}

object XmlStreamParser {
  def apply(): XmlStreamParser = new XmlStreamParser()
}
