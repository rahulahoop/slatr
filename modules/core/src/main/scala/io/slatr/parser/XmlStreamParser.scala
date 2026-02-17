package io.slatr.parser

import com.fasterxml.aalto.stax.InputFactoryImpl
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.Chunk

import java.io.{File, FileInputStream}
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import scala.collection.mutable
import scala.util.{Try, Using}

/** Streaming XML parser using Aalto XML */
class XmlStreamParser extends LazyLogging {
  
  private val inputFactory: XMLInputFactory = new InputFactoryImpl()
  
  /**
   * Parse XML file and extract elements as a stream of content maps.
   * Each map represents the parsed content of a depth-2 child of the root element.
   */
  def parse(file: File, chunk: Option[Chunk] = None): Try[Iterator[Map[String, Any]]] = {
    parseNamed(file, chunk).map(_.map(_._2))
  }
  
  /**
   * Parse XML file and return named elements: (elementName, parsedContent) pairs.
   * Each pair represents a depth-2 child of the root element.
   */
  def parseNamed(file: File, chunk: Option[Chunk] = None): Try[Iterator[(String, Map[String, Any])]] = {
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
 * Iterator that yields (elementName, parsedContent) pairs for depth-2 elements.
 */
private class XmlElementIterator(
  reader: XMLStreamReader,
  endOffset: Option[Long]
) extends Iterator[(String, Map[String, Any])] {
  
  private val stack = mutable.Stack[String]()
  private var current: Option[(String, Map[String, Any])] = None
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
  
  override def next(): (String, Map[String, Any]) = {
    if (!hasNext) throw new NoSuchElementException("No more elements")
    val result = current.get
    current = None
    result
  }
  
  private def parseElement(reader: XMLStreamReader): Map[String, Any] = {
    val builder = mutable.Map[String, Any]()
    val childElements = mutable.Map[String, mutable.ArrayBuffer[Any]]()
    
    for (i <- 0 until reader.getAttributeCount) {
      builder(s"@${reader.getAttributeLocalName(i)}") = reader.getAttributeValue(i)
    }
    
    var depth = 1
    val textContent = new StringBuilder()
    
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
    
    if (childElements.isEmpty && textContent.nonEmpty) {
      builder("#text") = textContent.toString.trim
    }
    
    childElements.foreach { case (name, values) =>
      builder(name) = values.toList
    }
    
    builder.toMap
  }
}

object XmlStreamParser {
  def apply(): XmlStreamParser = new XmlStreamParser()
}
