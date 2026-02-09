package io.slatr.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XsdSchemaSpec extends AnyFlatSpec with Matchers {
  
  "XsdElement" should "detect arrays from maxOccurs=unbounded" in {
    val element = XsdElement(
      name = "item",
      dataType = DataType.StringType,
      maxOccurs = None // None means unbounded
    )
    
    element.isArray shouldBe true
  }
  
  it should "detect arrays from maxOccurs > 1" in {
    val element = XsdElement(
      name = "item",
      dataType = DataType.StringType,
      maxOccurs = Some(5)
    )
    
    element.isArray shouldBe true
  }
  
  it should "not treat single occurrence as array" in {
    val element = XsdElement(
      name = "item",
      dataType = DataType.StringType,
      maxOccurs = Some(1)
    )
    
    element.isArray shouldBe false
  }
  
  it should "detect required elements from minOccurs > 0" in {
    val element = XsdElement(
      name = "item",
      dataType = DataType.StringType,
      minOccurs = 1
    )
    
    element.isRequired shouldBe true
  }
  
  it should "detect optional elements from minOccurs = 0" in {
    val element = XsdElement(
      name = "item",
      dataType = DataType.StringType,
      minOccurs = 0
    )
    
    element.isRequired shouldBe false
  }
}
