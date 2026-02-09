import sbt._

object Dependencies {
  val ScalaVersion = "2.13.12"
  
  // XML Processing
  val aaltoXml = "com.fasterxml" % "aalto-xml" % "1.3.2"
  val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  
  // XSD Processing & Validation
  val xerces = "xerces" % "xercesImpl" % "2.12.2"
  
  // HTTP Client (for XSD download)
  val sttp = "com.softwaremill.sttp.client3" %% "core" % "3.9.1"
  
  // CLI
  val decline = "com.monovore" %% "decline" % "2.4.1"
  val declineEffect = "com.monovore" %% "decline-effect" % "2.4.1"
  
  // Config & JSON
  val circeCore = "io.circe" %% "circe-core" % "0.14.6"
  val circeGeneric = "io.circe" %% "circe-generic" % "0.14.6"
  val circeParser = "io.circe" %% "circe-parser" % "0.14.6"
  val circeYaml = "io.circe" %% "circe-yaml" % "0.15.1"
  
  // Logging
  val logback = "ch.qos.logback" % "logback-classic" % "1.4.14"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  
  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.17" % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.17.0" % Test
  
  // Utilities
  val betterFiles = "com.github.pathikrit" %% "better-files" % "3.9.2"
  
  // Parquet
  val parquetHadoop = "org.apache.parquet" % "parquet-hadoop" % "1.13.1"
  val parquetAvro = "org.apache.parquet" % "parquet-avro" % "1.13.1"
  val hadoopClient = "org.apache.hadoop" % "hadoop-client" % "3.3.6" excludeAll(
    ExclusionRule(organization = "javax.servlet"),
    ExclusionRule(organization = "org.slf4j")
  )
  
  // BigQuery
  val bigquery = "com.google.cloud" % "google-cloud-bigquery" % "2.34.2"
  
  // Dependency groups
  val circeDeps = Seq(circeCore, circeGeneric, circeParser, circeYaml)
  val loggingDeps = Seq(logback, scalaLogging)
  val testDeps = Seq(scalaTest, scalaCheck)
  val parquetDeps = Seq(parquetHadoop, parquetAvro, hadoopClient)
  val bigqueryDeps = Seq(bigquery)
}
