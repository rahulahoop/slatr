package io.slatr.parser

import com.fasterxml.aalto.stax.InputFactoryImpl
import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.Chunk

import java.io.{File, FileInputStream}
import javax.xml.stream.events.XMLEvent
import javax.xml.stream.{XMLInputFactory, XMLStreamConstants, XMLStreamReader}
import scala.collection.mutable
import scala.util.{Try, Using}

/** Streaming XML parser using Aalto XML */
class XmlStreamParser extends LazyLogging {
  
  private val inputFactory: XMLInputFactory = new InputFactoryImpl()
  
  /**
   * Parse XML file and extract elements as a stream
   * @param file The XML file to parse
   * @param chunk Optional chunk specification for partial file reading
   * @return Iterator of parsed elements
   */
  def parse(file: File, chunk: Option[Chunk] = None): Try[Iterator[Map[String, Any]]] = {
    Try {
      val stream = new FileInputStream(file)
      val reader = inputFactory.createXMLStreamReader(stream)
      
      // Skip to chunk start offset if specified
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
 * Iterator that converts XML events to maps
 */
private class XmlElementIterator(
  reader: XMLStreamReader,
  endOffset: Option[Long]
) extends Iterator[Map[String, Any]] {
  
  private val stack = mutable.Stack[String]()
  private var currentElement: Option[Map[String, Any]] = None
  private var finished = false
  
  override def hasNext: Boolean = {
    if (finished) false
    else if (pastEndOffset) {
      finish()
      false
    }
    else if (currentElement.isDefined) true
    else {
      // Parse next element
      try {
        var foundEnd = false
        while (reader.hasNext && currentElement.isEmpty && !foundEnd) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT =>
              val elemName = reader.getLocalName
              stack.push(elemName)

              // Collect elements at depth 2 (children of root)
              if (stack.size == 2) {
                currentElement = Some(parseElement(reader, elemName))
                stack.pop()
              }

            case XMLStreamConstants.END_ELEMENT =>
              if (stack.nonEmpty) stack.pop()

            case XMLStreamConstants.END_DOCUMENT =>
              foundEnd = true

            case _ => // Ignore other event types
          }
        }

        if (currentElement.isEmpty) finish()
        currentElement.isDefined
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
  
  override def next(): Map[String, Any] = {
    if (!hasNext) throw new NoSuchElementException("No more elements")
    
    val result = currentElement.get
    currentElement = None
    result
  }
  
  private def parseElement(reader: XMLStreamReader, elementName: String): Map[String, Any] = {
    val builder = mutable.Map[String, Any]()
    val childElements = mutable.Map[String, mutable.ArrayBuffer[Any]]()
    
    // Parse attributes
    for (i <- 0 until reader.getAttributeCount) {
      val attrName = reader.getAttributeLocalName(i)
      val attrValue = reader.getAttributeValue(i)
      builder(s"@$attrName") = attrValue
    }
    
    var depth = 1
    var textContent = new StringBuilder()
    
    while (reader.hasNext && depth > 0) {
      reader.next() match {
        case XMLStreamConstants.START_ELEMENT =>
          depth += 1
          val childName = reader.getLocalName
          if (depth == 2) {
            val childValue = parseElement(reader, childName)
            val buffer = childElements.getOrElseUpdate(childName, mutable.ArrayBuffer())
            buffer += childValue
            depth -= 1 // parseElement already consumed the end element
          }
          
        case XMLStreamConstants.END_ELEMENT =>
          depth -= 1
          
        case XMLStreamConstants.CHARACTERS | XMLStreamConstants.CDATA =>
          val text = reader.getText
          if (text.trim.nonEmpty) {
            textContent.append(text)
          }
          
        case _ => // Ignore other event types
      }
    }
    
    // Add text content if present and no child elements
    if (childElements.isEmpty && textContent.nonEmpty) {
      builder("#text") = textContent.toString.trim
    }
    
    // Add child elements (always as arrays)
    childElements.foreach { case (name, values) =>
      builder(name) = values.toList
    }
    
    builder.toMap
  }
}

object XmlStreamParser {
  def apply(): XmlStreamParser = new XmlStreamParser()
}
