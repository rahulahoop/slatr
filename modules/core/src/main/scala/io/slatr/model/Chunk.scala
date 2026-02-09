package io.slatr.model

import java.io.File

/** Represents a chunk of an XML file for distributed processing */
case class Chunk(
  file: File,
  startOffset: Long,
  endOffset: Long,
  id: Int
) {
  def size: Long = endOffset - startOffset
}

object Chunk {
  /** Create a single chunk for the entire file */
  def singleChunk(file: File): Chunk = {
    Chunk(file, 0, file.length(), 0)
  }
}
