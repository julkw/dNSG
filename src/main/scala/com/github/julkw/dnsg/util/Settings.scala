package com.github.julkw.dnsg.util

import com.typesafe.config.Config

case class Settings(config: Config) {

  private val namespace = "com.github.julkw.dnsg"

  val inputFilePath: String = config.getString(s"$namespace.input-file")

  val k: Int = config.getInt(s"$namespace.k")

  val maxReverseNeighbors: Int= config.getInt(s"$namespace.max-reverse-neighbors")

  val workers: Int = config.getInt(s"$namespace.workers")

  // for testing
  val queryFilePath: String = config.getString(s"$namespace.query-testing.query-file")

  val linesOffset: Int = config.getInt(s"$namespace.query-testing.lines-offset")

  val lines: Int = config.getInt(s"$namespace.query-testing.lines")

  val dimensionOffset: Int = config.getInt(s"$namespace.query-testing.dimension-offset")

  val dimensions: Int = config.getInt(s"$namespace.query-testing.dimensions")
}
