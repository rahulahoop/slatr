package io.slatr.cli

import com.monovore.decline._
import io.slatr.cli.commands.{BigQueryCommand, ConvertCommand, InferSchemaCommand, XsdInfoCommand}
import cats.implicits._

object Main extends CommandApp(
  name = "slatr",
  header = "XML to modern format converter",
  version = "0.1.0-SNAPSHOT",
  main = {
    val commands = Opts.subcommands(
      ConvertCommand.command,
      InferSchemaCommand.command,
      XsdInfoCommand.command,
      BigQueryCommand.command
    )
    
    commands
  }
)
