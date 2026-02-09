package io.slatr.schema

import com.typesafe.scalalogging.LazyLogging
import io.slatr.model.{XsdConfig, XsdSchema}
import sttp.client3._

import java.io.File
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

/** Resolves and caches XSD schemas with in-memory storage */
class XsdResolver(config: XsdConfig) extends LazyLogging {
  
  // In-memory cache: URL -> XSD Schema
  private val cache = TrieMap[String, XsdSchema]()
  
  /**
   * Resolve XSD from XML file header
   * @param xmlFile The XML file to extract XSD URL from
   * @return Optional XSD schema if found and successfully downloaded
   */
  def resolveFromXml(xmlFile: File, parser: io.slatr.parser.XmlStreamParser): Option[XsdSchema] = {
    if (!config.enabled) {
      logger.debug("XSD resolution disabled in config")
      return None
    }
    
    parser.extractXsdUrl(xmlFile) match {
      case Some(url) =>
        logger.info(s"Found XSD URL in XML header: $url")
        resolve(url)
      case None =>
        logger.debug("No XSD URL found in XML header")
        None
    }
  }
  
  /**
   * Resolve XSD from explicit URL
   * @param url The XSD URL
   * @return Optional XSD schema if successfully downloaded and parsed
   */
  def resolve(url: String): Option[XsdSchema] = {
    cache.get(url) match {
      case Some(schema) =>
        logger.debug(s"XSD cache hit: $url")
        Some(schema)
      case None =>
        logger.info(s"XSD cache miss, downloading: $url")
        downloadAndParse(url) match {
          case Success(schema) =>
            cache.put(url, schema)
            Some(schema)
          case Failure(ex) =>
            logger.error(s"Failed to download/parse XSD from $url: ${ex.getMessage}")
            None
        }
    }
  }
  
  /**
   * Download XSD content from URL
   */
  private def downloadAndParse(url: String): Try[XsdSchema] = Try {
    val backend = HttpURLConnectionBackend()
    
    try {
      val request = basicRequest
        .get(uri"$url")
        .readTimeout(scala.concurrent.duration.Duration(config.timeout, "seconds"))
      
      val response = request.send(backend)
      
      response.code.code match {
        case code if code >= 200 && code < 300 =>
          val xsdContent = response.body match {
            case Right(content) => content
            case Left(error) => throw new Exception(s"Failed to read response body: $error")
          }
          
          logger.debug(s"Successfully downloaded XSD from $url (${xsdContent.length} bytes)")
          
          // Parse XSD content
          val xsdParser = new XsdParser()
          xsdParser.parse(xsdContent, url)
          
        case code =>
          throw new Exception(s"HTTP error $code when downloading XSD from $url")
      }
    } finally {
      backend.close()
    }
  }
  
  /**
   * Clear the in-memory cache
   */
  def clearCache(): Unit = {
    logger.info(s"Clearing XSD cache (${cache.size} entries)")
    cache.clear()
  }
  
  /**
   * Get cache statistics
   */
  def getCacheStats: Map[String, Int] = {
    Map("cacheSize" -> cache.size)
  }
}

object XsdResolver {
  def apply(config: XsdConfig): XsdResolver = new XsdResolver(config)
}
