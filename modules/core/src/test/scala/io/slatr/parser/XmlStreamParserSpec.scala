package io.slatr.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class XmlStreamParserSpec extends AnyFlatSpec with Matchers {
  
  val parser = new XmlStreamParser()
  
  def getTestResource(name: String): File = {
    new File(getClass.getResource(s"/$name").toURI)
  }
  
  "XmlStreamParser" should "parse simple XML file" in {
    val file = getTestResource("test-simple.xml")
    val result = parser.parse(file, None)
    
    result.isSuccess shouldBe true
    val elements = result.get.toList
    
    elements should have size 2
    elements.head should contain key "title"
    elements.head should contain key "author"
  }
  
  it should "extract root element name" in {
    val file = getTestResource("test-simple.xml")
    val rootElement = parser.getRootElementName(file)
    
    rootElement shouldBe Some("catalog")
  }
  
  it should "extract XSD URL from XML header" in {
    val file = getTestResource("test-with-xsd.xml")
    val xsdUrl = parser.extractXsdUrl(file)
    
    xsdUrl shouldBe Some("http://example.com/test.xsd")
  }
  
  it should "return None when no XSD URL is present" in {
    val file = getTestResource("test-simple.xml")
    val xsdUrl = parser.extractXsdUrl(file)
    
    xsdUrl shouldBe None
  }
  
  it should "parse nested XML structures" in {
    val file = getTestResource("test-nested.xml")
    val result = parser.parse(file, None)
    
    result.isSuccess shouldBe true
    val elements = result.get.toList
    
    elements should have size 1
    val employee = elements.head
    
    employee should contain key "id"
    employee should contain key "name"
    employee should contain key "contact"
  }
  
  it should "handle single-item lists" in {
    val file = getTestResource("test-single-item.xml")
    val result = parser.parse(file, None)
    
    result.isSuccess shouldBe true
    val elements = result.get.toList
    
    elements should have size 1
    val record = elements.head
    
    // Tags should be a list even with single item
    record should contain key "tags"
    val tags = record("tags").asInstanceOf[List[_]]
    tags should have size 1
  }

  it should "always represent child elements as arrays regardless of count" in {
    val file = getTestResource("test-array-consistency.xml")
    val result = parser.parse(file, None)

    result.isSuccess shouldBe true
    val items = result.get.toList
    items should have size 3

    // --- Item 0: multiple <tag> children inside <tags> ---
    val multi = items(0)
    val multiName = multi("name").asInstanceOf[List[Map[String, Any]]].head("#text")
    multiName shouldBe "Item with multiple tags"

    val multiTags = multi("tags").asInstanceOf[List[Map[String, Any]]]
    multiTags should have size 1 // one <tags> element
    val multiTagList = multiTags.head("tag").asInstanceOf[List[Map[String, Any]]]
    multiTagList should have size 2
    multiTagList.map(_("#text")) shouldBe List("urgent", "review")

    // --- Item 1: single <tag> child inside <tags> ---
    val single = items(1)
    val singleName = single("name").asInstanceOf[List[Map[String, Any]]].head("#text")
    singleName shouldBe "Item with single tag"

    val singleTags = single("tags").asInstanceOf[List[Map[String, Any]]]
    singleTags should have size 1
    val singleTagList = singleTags.head("tag").asInstanceOf[List[Map[String, Any]]]
    singleTagList should have size 1 // MUST still be a List, not a scalar
    singleTagList.head("#text") shouldBe "archived"

    // Key assertion: both multi and single <tag> entries have the same type (List)
    multiTagList shouldBe a[List[_]]
    singleTagList shouldBe a[List[_]]

    // --- Item 2: no <tags> element at all ---
    val noTags = items(2)
    noTags should not contain key ("tags")
  }
  
  it should "extract text content from simple elements" in {
    val file = getTestResource("test-simple.xml")
    val result = parser.parse(file, None)
    
    result.isSuccess shouldBe true
    val elements = result.get.toList
    val firstBook = elements.head
    
    val titleList = firstBook("title").asInstanceOf[List[_]]
    val titleMap = titleList.head.asInstanceOf[Map[String, Any]]
    titleMap("#text") shouldBe "Test Book 1"
  }
  
  it should "parse multiple elements correctly" in {
    val file = getTestResource("test-simple.xml")
    val result = parser.parse(file, None)
    
    result.isSuccess shouldBe true
    val elements = result.get.toList
    
    elements should have size 2
    
    // Verify first book
    val firstBook = elements.head
    val firstTitle = firstBook("title").asInstanceOf[List[_]].head.asInstanceOf[Map[String, Any]]
    firstTitle("#text") shouldBe "Test Book 1"
    
    // Verify second book
    val secondBook = elements(1)
    val secondTitle = secondBook("title").asInstanceOf[List[_]].head.asInstanceOf[Map[String, Any]]
    secondTitle("#text") shouldBe "Test Book 2"
  }
}
