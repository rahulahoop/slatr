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
   * Extract XSD URL from XML header
   */
  def extractXsdUrl(file: File): Option[String] = {
    Using(new FileInputStream(file)) { stream =>
      val reader = inputFactory.createXMLStreamReader(stream)
      
      try {
        while (reader.hasNext) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT =>
              // Check for xsi:schemaLocation attribute
              val attributeCount = reader.getAttributeCount
              for (i <- 0 until attributeCount) {
                val attrName = reader.getAttributeLocalName(i)
                if (attrName == "schemaLocation") {
                  val value = reader.getAttributeValue(i)
                  // schemaLocation format: "namespace url" - we want the URL (second one)
                  val urls = value.split("\\s+").filter(_.startsWith("http"))
                  if (urls.length >= 2) {
                    return Some(urls(1)) // Take the schema URL, not the namespace
                  } else if (urls.nonEmpty) {
                    return Some(urls.head)
                  }
                } else if (attrName == "noNamespaceSchemaLocation") {
                  val value = reader.getAttributeValue(i)
                  // noNamespaceSchemaLocation format: just the URL
                  if (value.startsWith("http")) {
                    return Some(value.trim)
                  }
                }
              }
              // Only check root element
              return None
            case _ => // Continue
          }
        }
        None
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
        while (reader.hasNext) {
          reader.next() match {
            case XMLStreamConstants.START_ELEMENT =>
              return Some(reader.getLocalName)
            case _ => // Continue
          }
        }
        None
      } finally {
        reader.close()
      }
    }.toOption.flatten
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
    if (finished) return false
    
    // Check if we've reached the end offset
    endOffset.foreach { end =>
      val currentPos = reader.getLocation.getCharacterOffset
      if (currentPos >= end) {
        finished = true
        reader.close()
        return false
      }
    }
    
    if (currentElement.isDefined) return true
    
    // Parse next element
    try {
      while (reader.hasNext && currentElement.isEmpty) {
        reader.next() match {
          case XMLStreamConstants.START_ELEMENT =>
            val elemName = reader.getLocalName
            stack.push(elemName)
            
            // For now, we'll collect elements at depth 2 (children of root)
            if (stack.size == 2) {
              currentElement = Some(parseElement(reader, elemName))
              stack.pop()
            }
            
          case XMLStreamConstants.END_ELEMENT =>
            if (stack.nonEmpty) stack.pop()
            
          case XMLStreamConstants.END_DOCUMENT =>
            finished = true
            reader.close()
            return false
            
          case _ => // Ignore other event types
        }
      }
      
      if (currentElement.isEmpty) {
        finished = true
        reader.close()
      }
      
      currentElement.isDefined
    } catch {
      case e: Exception =>
        reader.close()
        throw e
    }
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
