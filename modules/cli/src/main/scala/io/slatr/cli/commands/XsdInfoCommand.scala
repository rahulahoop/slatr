package io.slatr.cli.commands

import com.monovore.decline._
import io.slatr.model.XsdConfig
import io.slatr.parser.XmlStreamParser
import io.slatr.schema.XsdResolver

import java.io.File
import cats.implicits._

object XsdInfoCommand {
  
  val inputOpt: Opts[File] = Opts.argument[String]("input")
    .mapValidated(s => new File(s).validNel)
    .validate("Input file must exist")(_.exists())
  
  val command: Command[Unit] = Command(
    name = "xsd-info",
    header = "Display XSD information from XML file header"
  ) {
    inputOpt.map { input =>
      
      println(s"Extracting XSD information from ${input.getName}")
      
      val xmlParser = XmlStreamParser()
      val xsdResolver = XsdResolver(XsdConfig())
      
      // Extract XSD URL
      xmlParser.extractXsdUrl(input) match {
        case Some(url) =>
          println(s"\nXSD URL Found: $url")
          
          // Try to resolve XSD
          println("Attempting to download and parse XSD...")
          xsdResolver.resolve(url) match {
            case Some(xsdSchema) =>
              println(s"✓ Successfully downloaded and parsed XSD")
              println(s"  Target Namespace: ${xsdSchema.targetNamespace.getOrElse("(none)")}")
              println(s"  Elements Defined: ${xsdSchema.elements.size}")
              
              if (xsdSchema.elements.nonEmpty) {
                println("\nElement Details:")
                xsdSchema.elements.toSeq.sortBy(_._1).take(10).foreach { case (name, elem) =>
                  val arrayMarker = if (elem.isArray) "[]" else ""
                  val requiredMarker = if (elem.isRequired) " (required)" else ""
                  println(s"  $name: ${elem.dataType}$arrayMarker$requiredMarker")
                }
                
                if (xsdSchema.elements.size > 10) {
                  println(s"  ... and ${xsdSchema.elements.size - 10} more elements")
                }
              }
              
              // Show cache stats
              val stats = xsdResolver.getCacheStats
              println(s"\nCache Stats: ${stats("cacheSize")} entries")
              
            case None =>
              println(s"✗ Failed to download or parse XSD from $url")
          }
          
        case None =>
          println("✗ No XSD URL found in XML header")
          println("The XML file does not reference an XSD schema in its header.")
      }
    }
  }
}
