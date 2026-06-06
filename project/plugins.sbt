// Assembly plugin for creating fat JARs
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")

// Code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Linting / refactoring (RemoveUnused imports)
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.6")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.9")

// Native packager for CLI distribution
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")
