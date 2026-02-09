import Dependencies._

ThisBuild / organization := "io.slatr"
ThisBuild / scalaVersion := Dependencies.ScalaVersion
ThisBuild / version      := "0.1.0-SNAPSHOT"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

// Root project
lazy val root = (project in file("."))
  .settings(
    name := "slatr",
    publish / skip := true
  )
  .aggregate(core, cli)

// Core library module
lazy val core = (project in file("modules/core"))
  .settings(
    name := "slatr-core",
    libraryDependencies ++= Seq(
      aaltoXml,
      scalaXml,
      xerces,
      sttp,
      betterFiles
    ) ++ circeDeps ++ loggingDeps ++ parquetDeps ++ testDeps
  )

// CLI module
lazy val cli = (project in file("modules/cli"))
  .settings(
    name := "slatr-cli",
    libraryDependencies ++= Seq(
      decline,
      declineEffect
    ) ++ loggingDeps ++ testDeps,
    assembly / mainClass := Some("io.slatr.cli.Main"),
    assembly / assemblyJarName := "slatr.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )
  .enablePlugins(JavaAppPackaging)
  .dependsOn(core)
