package io.slatr.model

/** Represents the complete schema of an XML document */
case class Schema(
  rootElement: String,
  fields: Map[String, Field]
) {
  
  /** Add or update a field in the schema */
  def withField(name: String, field: Field): Schema = {
    copy(fields = fields + (name -> field))
  }
  
  /** Merge with another schema, preferring this schema's types on conflict */
  def merge(other: Schema): Schema = {
    val mergedFields = other.fields.foldLeft(fields) { case (acc, (name, field)) =>
      acc.get(name) match {
        case Some(existingField) =>
          // If field exists, keep existing (first schema takes precedence)
          acc
        case None =>
          // Add new field
          acc + (name -> field)
      }
    }
    copy(fields = mergedFields)
  }
}

object Schema {
  def empty(rootElement: String): Schema = Schema(rootElement, Map.empty)
}
