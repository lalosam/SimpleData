package org.rojosam
package etl.jobs

object JobFactory {

  def apply(jobType: String, jobName: String, args: Array[String]): JobBase = {
    jobType match {
      case "DataLake" => new DataLakeGlueJob(jobName, args)
      case "RestoreMetaData" => new RestoreMetaDataGlueJob(jobName, args)
    }
  }

}
