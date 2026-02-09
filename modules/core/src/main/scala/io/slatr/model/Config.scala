package io.slatr.model

/** Configuration for XML conversion */
case class SlatrConfig(
  input: InputConfig,
  schema: SchemaConfig,
  chunking: ChunkingConfig,
  output: OutputConfig,
  logging: LoggingConfig = LoggingConfig(),
  bigquery: Option[BigQueryConfig] = None
)

case class InputConfig(
  path: String,
  encoding: String = "UTF-8"
)

case class SchemaConfig(
  mode: SchemaMode = SchemaMode.Hybrid,
  xsd: XsdConfig = XsdConfig(),
  sampling: SamplingConfig = SamplingConfig(),
  overrides: SchemaOverrides = SchemaOverrides()
)

sealed trait SchemaMode
object SchemaMode {
  case object Auto extends SchemaMode    // Auto-infer only
  case object Xsd extends SchemaMode     // XSD only
  case object Manual extends SchemaMode  // Manual overrides only
  case object Hybrid extends SchemaMode  // Combine all sources
  
  def fromString(s: String): SchemaMode = s.toLowerCase match {
    case "auto" => Auto
    case "xsd" => Xsd
    case "manual" => Manual
    case "hybrid" => Hybrid
    case _ => Hybrid
  }
}

case class XsdConfig(
  enabled: Boolean = true,
  timeout: Int = 30,
  validate: Boolean = false,
  followImports: Boolean = false
)

case class SamplingConfig(
  size: Int = 1000
)

case class SchemaOverrides(
  forceArrays: Seq[String] = Seq.empty,
  typeHints: Map[String, String] = Map.empty
)

case class ChunkingConfig(
  enabled: Boolean = false,
  chunkSize: String = "128MB",
  preferBoundaries: Boolean = true
) {
  def chunkSizeBytes: Long = {
    val size = chunkSize.toUpperCase
    if (size.endsWith("MB")) {
      size.dropRight(2).toLong * 1024 * 1024
    } else if (size.endsWith("GB")) {
      size.dropRight(2).toLong * 1024 * 1024 * 1024
    } else if (size.endsWith("KB")) {
      size.dropRight(2).toLong * 1024
    } else {
      size.toLong
    }
  }
}

case class OutputConfig(
  format: OutputFormat = OutputFormat.Json,
  path: String,
  pretty: Boolean = true,
  compression: Option[String] = None
)

sealed trait OutputFormat
object OutputFormat {
  case object Json extends OutputFormat
  case object JsonLines extends OutputFormat
  case object Avro extends OutputFormat
  case object Parquet extends OutputFormat
  
  def fromString(s: String): OutputFormat = s.toLowerCase match {
    case "json" => Json
    case "jsonl" | "jsonlines" => JsonLines
    case "avro" => Avro
    case "parquet" => Parquet
    case _ => Json
  }
}

case class LoggingConfig(
  level: String = "info"
)

case class BigQueryConfig(
  project: String,
  dataset: String,
  table: String,
  location: String = "US",
  writeMode: WriteMode = WriteMode.Append,
  createTable: Boolean = true,
  credentials: Option[String] = None // Path to service account JSON
)

sealed trait WriteMode
object WriteMode {
  case object Append extends WriteMode
  case object Overwrite extends WriteMode
  case object ErrorIfExists extends WriteMode
  
  def fromString(s: String): WriteMode = s.toLowerCase match {
    case "append" => Append
    case "overwrite" => Overwrite
    case "error" => ErrorIfExists
    case _ => Append
  }
}
