package io.slatr.model

/** Represents a field in the schema */
case class Field(
  name: String,
  dataType: DataType,
  nullable: Boolean = true,
  isArray: Boolean = false
)
