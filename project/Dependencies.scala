import sbt._

object Dependencies {
  val ScalaVersion = "2.13.18"
  
  // XML Processing
  val aaltoXml = "com.fasterxml" % "aalto-xml" % "1.4.0"
  val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.4.0"
  
  // XSD Processing & Validation
  val xerces = "xerces" % "xercesImpl" % "2.12.2"
  
  // HTTP Client (for XSD download)
  val sttp = "com.softwaremill.sttp.client3" %% "core" % "3.9.1"
  
  // CLI
  val decline = "com.monovore" %% "decline" % "2.4.1"
  val declineEffect = "com.monovore" %% "decline-effect" % "2.4.1"
  
  // Config & JSON
  val upickle = "com.lihaoyi" %% "upickle" % "4.3.2"
  
  // Logging
  val logback = "ch.qos.logback" % "logback-classic" % "1.4.14"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  
  // Testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.20" % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.19.0" % Test
  
  // Integration Testing
  val testcontainers = "org.testcontainers" % "testcontainers" % "2.0.5" % "it,test"
  val testcontainersScalatest = "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.44.1" % "it,test"
  
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
  val bigquery = "com.google.cloud" % "google-cloud-bigquery" % "2.67.0"
  
  // PostgreSQL
  val postgresql = "org.postgresql" % "postgresql" % "42.7.1"
  val testcontainersPostgres = "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.44.1" % "it,test"
  
  // Dependency groups
  val jsonDeps = Seq(upickle)
  val loggingDeps = Seq(logback, scalaLogging)
  val testDeps = Seq(scalaTest, scalaCheck)
  val integrationTestDeps = Seq(testcontainers, testcontainersScalatest, testcontainersPostgres, scalaTest, scalaCheck)
  val parquetDeps = Seq(parquetHadoop, parquetAvro, hadoopClient)
  val bigqueryDeps = Seq(bigquery)
  val postgresqlDeps = Seq(postgresql)
}
