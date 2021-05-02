package org.rojosam
package etl.jobs

import com.amazonaws.services.glue.GlueContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

abstract class JobBase(jobName: String) {

  protected val sparkCtx: SparkContext = new SparkContext()
  protected val glueContext: GlueContext = new GlueContext(sparkCtx)
  glueContext.setConf("spark.sql.parquet.compression.codec", "gzip")
  glueContext.setConf("spark.sql.parquet.writeLegacyFormat", "true")

  protected val spark:SparkSession = glueContext.getSparkSession

  def execute()

}