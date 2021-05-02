package org.rojosam
package etl.transformations

import java.time.LocalDateTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import etl.utils.DateTimeUtils
import org.slf4j.{Logger, LoggerFactory}

class Transformation(df:DataFrame) {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def getDF:DataFrame = df

  def addDateColumns(timestampColumn:String):Transformation = {
    Transformation(df.withColumn("dl_year", year(col(timestampColumn)).cast(StringType))
      .withColumn("dl_month", lpad(month(col(timestampColumn)), 2, "0"))
      .withColumn("dl_day", lpad(dayofmonth(col(timestampColumn)), 2, "0")))
  }

  def addTimestamp(timestampColumnName: String): Transformation = {
    val timestampValue = DateTimeUtils.getNowUTC
    addTimestamp(timestampColumnName, timestampValue)
  }

  def addTimestamp(timestampColumnName:String, dateTime:LocalDateTime): Transformation = {
    val timestampValue = DateTimeUtils.getDateTimeInDMSFormat(dateTime)
    Transformation(df.withColumn(timestampColumnName, lit(timestampValue)))
  }

  def dropDateColumns():Transformation = {
    Transformation(df.drop("dl_year", "dl_month", "dl_day"))
  }

  def prependColumn(columnName:String, value:Any): Transformation = {
    val previousColumns = df.columns
    if(previousColumns.contains(columnName)) throw new RuntimeException(s"Duplicated column [$columnName]")
    Transformation(
      df.withColumn(columnName, lit(value)).select(columnName, previousColumns:_*)
    )
  }

  def repartition(recordsByPartition:Double): Transformation = {
    val numRecords = df.count()
    val partitions = scala.math.max(scala.math.ceil(numRecords / recordsByPartition).toInt,1)
    log.info(s"[$numRecords] records will be repartitioned into [$partitions] partitions")
    Transformation(df.repartition(partitions))
  }

  def replaceNulls(column:String, value:String): Transformation = {
    Transformation(
      df.withColumn(column, when(col(column).isNull, value).otherwise(col(column)))
    )
  }

}

object Transformation {
  def apply(df: DataFrame): Transformation = new Transformation(df)
}

