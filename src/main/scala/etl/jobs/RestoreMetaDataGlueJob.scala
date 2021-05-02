package org.rojosam
package etl.jobs

import org.slf4j.{Logger, LoggerFactory}

class RestoreMetaDataGlueJob(jobName: String, args: Array[String]) extends JobBase(jobName) {

  import spark.implicits._

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def execute(): Unit = {

  }
}