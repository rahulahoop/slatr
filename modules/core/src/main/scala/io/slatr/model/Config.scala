package io.slatr.model

import upickle.default.{ReadWriter, macroRW, readwriter}

/** Configuration for XML conversion */
case class SlatrConfig(
  input: InputConfig,
  schema: SchemaConfig,
  chunking: ChunkingConfig,
  output: OutputConfig,
  logging: LoggingConfig = LoggingConfig(),
  bigquery: Option[BigQueryConfig] = None,
  postgresql: Option[PostgreSQLConfig] = None
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

  implicit val rw: ReadWriter[SchemaMode] = readwriter[String].bimap[SchemaMode](
    {
      case Auto   => "auto"
      case Xsd    => "xsd"
      case Manual => "manual"
      case Hybrid => "hybrid"
    },
    fromString
  )
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

  implicit val rw: ReadWriter[OutputFormat] = readwriter[String].bimap[OutputFormat](
    {
      case Json      => "json"
      case JsonLines => "jsonl"
      case Avro      => "avro"
      case Parquet   => "parquet"
    },
    fromString
  )
}

case class LoggingConfig(
  level: String = "info"
)

case class BigQueryConfig(
  projectId: String,
  datasetId: String,
  tableId: String,
  location: String = "US",
  writeMode: WriteMode = WriteMode.Append,
  credentialsPath: Option[String] = None, // Path to service account JSON
  useFirebaseModel: Boolean = false // Use Firebase-style array of key-value structs
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

  implicit val rw: ReadWriter[WriteMode] = readwriter[String].bimap[WriteMode](
    {
      case Append        => "append"
      case Overwrite     => "overwrite"
      case ErrorIfExists => "error"
    },
    fromString
  )
}

case class PostgreSQLConfig(
  host: String = "localhost",
  port: Int = 5432,
  database: String,
  schema: String = "public",
  table: String,
  username: String,
  password: String,
  writeMode: WriteMode = WriteMode.Append,
  useFirebaseModel: Boolean = false // Use Firebase-style JSONB key-value storage
)

// upickle JSON ReadWriters. Case-class macros honour the default arguments above, so config
// files may omit any field. Sealed-trait codecs map to/from their lowercase string labels.
object SlatrConfig      { implicit val rw: ReadWriter[SlatrConfig]      = macroRW }
object InputConfig      { implicit val rw: ReadWriter[InputConfig]      = macroRW }
object SchemaConfig     { implicit val rw: ReadWriter[SchemaConfig]     = macroRW }
object XsdConfig        { implicit val rw: ReadWriter[XsdConfig]        = macroRW }
object SamplingConfig   { implicit val rw: ReadWriter[SamplingConfig]   = macroRW }
object SchemaOverrides  { implicit val rw: ReadWriter[SchemaOverrides]  = macroRW }
object ChunkingConfig   { implicit val rw: ReadWriter[ChunkingConfig]   = macroRW }
object OutputConfig     { implicit val rw: ReadWriter[OutputConfig]     = macroRW }
object LoggingConfig    { implicit val rw: ReadWriter[LoggingConfig]    = macroRW }
object BigQueryConfig   { implicit val rw: ReadWriter[BigQueryConfig]   = macroRW }
object PostgreSQLConfig { implicit val rw: ReadWriter[PostgreSQLConfig] = macroRW }
