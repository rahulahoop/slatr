package io.slatr.model

/**
 * A parsed XML element — the typed replacement for the old untyped string-keyed map.
 *
 * @param attributes element attributes (XML `name="value"`); was the `"@name"` entries
 * @param text       character content, present only for leaf elements; was the `"#text"` entry
 * @param children   child elements keyed by tag name, in document order per name;
 *                   was the `"childName"` entries holding lists of nested elements
 */
final case class XmlElement(
  attributes: Map[String, String] = Map.empty,
  text: Option[String] = None,
  children: Map[String, List[XmlElement]] = Map.empty
) {

  /** True when this element has no attributes, text, or children. */
  def isEmpty: Boolean = attributes.isEmpty && text.isEmpty && children.isEmpty

  /** The first child element for `name`, if any. */
  def child(name: String): Option[XmlElement] = children.get(name).flatMap(_.headOption)

  /** Follow a path of single child elements and return the deepest element's text. */
  def textAt(path: String*): Option[String] =
    path.toList match {
      case Nil          => text
      case head :: tail => child(head).flatMap(_.textAt(tail: _*))
    }
}
