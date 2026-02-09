package io.slatr.schema

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{DataType, XsdElement, XsdSchema}

import java.io.StringReader
import javax.xml.parsers.DocumentBuilderFactory
import org.w3c.dom.{Document, Element, Node, NodeList}
import org.xml.sax.InputSource
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Parser for XSD (XML Schema Definition) files */
class XsdParser extends LazyLogging {
  
  private val XS_NAMESPACE = "http://www.w3.org/2001/XMLSchema"
  
  /**
   * Parse XSD content into XsdSchema model
   * @param xsdContent The XSD file content as string
   * @param url The source URL (for reference)
   * @return Parsed XSD schema
   */
  def parse(xsdContent: String, url: String): XsdSchema = {
    logger.debug(s"Parsing XSD from $url")
    
    val factory = DocumentBuilderFactory.newInstance()
    factory.setNamespaceAware(true)
    val builder = factory.newDocumentBuilder()
    val doc: Document = builder.parse(new InputSource(new StringReader(xsdContent)))
    
    val schemaElement = doc.getDocumentElement
    val targetNamespace = Option(schemaElement.getAttribute("targetNamespace"))
      .filter(_.nonEmpty)
    
    // Parse element definitions
    val elements = parseElements(doc)
    
    logger.info(s"Parsed XSD from $url: ${elements.size} elements, targetNamespace=$targetNamespace")
    
    XsdSchema(url, elements, targetNamespace)
  }
  
  /**
   * Parse all element definitions from the XSD document
   */
  private def parseElements(doc: Document): Map[String, XsdElement] = {
    val elements = mutable.Map[String, XsdElement]()
    
    // Get all xs:element nodes
    val elementNodes = doc.getElementsByTagNameNS(XS_NAMESPACE, "element")
    
    for (i <- 0 until elementNodes.getLength) {
      val elementNode = elementNodes.item(i).asInstanceOf[Element]
      parseElement(elementNode).foreach { elem =>
        elements(elem.name) = elem
      }
    }
    
    // Also parse elements within complexTypes
    val complexTypeNodes = doc.getElementsByTagNameNS(XS_NAMESPACE, "complexType")
    for (i <- 0 until complexTypeNodes.getLength) {
      val complexTypeNode = complexTypeNodes.item(i).asInstanceOf[Element]
      parseComplexType(complexTypeNode, None).foreach { case (name, elem) =>
        elements(name) = elem
      }
    }
    
    elements.toMap
  }
  
  /**
   * Parse a single xs:element node
   */
  private def parseElement(elementNode: Element): Option[XsdElement] = {
    val name = elementNode.getAttribute("name")
    if (name.isEmpty) return None
    
    val typeName = elementNode.getAttribute("type")
    val dataType = if (typeName.nonEmpty) {
      DataType.fromXsdType(typeName)
    } else {
      // Check for inline complexType or simpleType
      val complexTypes = elementNode.getElementsByTagNameNS(XS_NAMESPACE, "complexType")
      if (complexTypes.getLength > 0) {
        // Has inline complex type - treat as struct
        val fields = parseComplexType(complexTypes.item(0).asInstanceOf[Element], None)
        DataType.StructType(fields.map { case (n, elem) =>
          n -> io.slatr.model.Field(n, elem.dataType, !elem.isRequired, elem.isArray)
        })
      } else {
        DataType.StringType // Default
      }
    }
    
    val minOccurs = Option(elementNode.getAttribute("minOccurs"))
      .filter(_.nonEmpty)
      .map(_.toInt)
      .getOrElse(1)
    
    val maxOccurs = Option(elementNode.getAttribute("maxOccurs"))
      .filter(_.nonEmpty)
      .flatMap {
        case "unbounded" => None
        case num => Some(num.toInt)
      }
      .orElse(Some(1))
    
    val nillable = Option(elementNode.getAttribute("nillable"))
      .filter(_.nonEmpty)
      .exists(_.toBoolean)
    
    Some(XsdElement(name, dataType, minOccurs, maxOccurs, nillable))
  }
  
  /**
   * Parse elements within a complexType
   */
  private def parseComplexType(
    complexTypeNode: Element,
    parentName: Option[String]
  ): Map[String, XsdElement] = {
    val elements = mutable.Map[String, XsdElement]()
    
    // Check for xs:sequence, xs:choice, xs:all
    val sequences = complexTypeNode.getElementsByTagNameNS(XS_NAMESPACE, "sequence")
    val choices = complexTypeNode.getElementsByTagNameNS(XS_NAMESPACE, "choice")
    val alls = complexTypeNode.getElementsByTagNameNS(XS_NAMESPACE, "all")
    
    val containers = List(sequences, choices, alls)
    
    containers.foreach { nodeList =>
      for (i <- 0 until nodeList.getLength) {
        val container = nodeList.item(i).asInstanceOf[Element]
        val childElements = container.getElementsByTagNameNS(XS_NAMESPACE, "element")
        
        for (j <- 0 until childElements.getLength) {
          val childElement = childElements.item(j).asInstanceOf[Element]
          parseElement(childElement).foreach { elem =>
            elements(elem.name) = elem
          }
        }
      }
    }
    
    elements.toMap
  }
}

object XsdParser {
  def apply(): XsdParser = new XsdParser()
}
