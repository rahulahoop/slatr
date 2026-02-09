package io.slatr.model

/** Represents a parsed XSD schema */
case class XsdSchema(
  url: String,
  elements: Map[String, XsdElement],
  targetNamespace: Option[String] = None
)

/** Represents an element definition from XSD */
case class XsdElement(
  name: String,
  dataType: DataType,
  minOccurs: Int = 1,
  maxOccurs: Option[Int] = Some(1), // None means unbounded
  isNillable: Boolean = false
) {
  
  /** Check if this element should be treated as an array */
  def isArray: Boolean = maxOccurs match {
    case None => true // unbounded
    case Some(max) => max > 1
  }
  
  /** Check if this element is required */
  def isRequired: Boolean = minOccurs > 0
}
