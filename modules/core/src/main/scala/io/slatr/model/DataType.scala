package io.slatr.model

/** Represents the data type of a field in the schema */
sealed trait DataType

object DataType {
  case object StringType extends DataType
  case object IntType extends DataType
  case object LongType extends DataType
  case object DoubleType extends DataType
  case object BooleanType extends DataType
  case object TimestampType extends DataType
  case object DateType extends DataType
  case object TimeType extends DataType
  case class DecimalType(precision: Int, scale: Int) extends DataType
  case class ArrayType(elementType: DataType) extends DataType
  case class StructType(fields: Map[String, Field]) extends DataType
  
  /** Convert XSD type string to DataType */
  def fromXsdType(xsdType: String): DataType = xsdType match {
    case "string" | "xs:string" => StringType
    case "int" | "xs:int" | "integer" | "xs:integer" => IntType
    case "long" | "xs:long" => LongType
    case "double" | "xs:double" | "float" | "xs:float" => DoubleType
    case "boolean" | "xs:boolean" => BooleanType
    case "dateTime" | "xs:dateTime" => TimestampType
    case "date" | "xs:date" => DateType
    case "time" | "xs:time" => TimeType
    case "decimal" | "xs:decimal" => DecimalType(10, 2) // Default precision
    case _ => StringType // Default to string for unknown types
  }
}
