package org.rojosam
package etl.utils

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.{Date, TimeZone}

object DateTimeUtils {

  def splitDate(date: String): (String, String, String) = {
    if(date.length == 8) {
      (date.substring(0, 4), date.substring(4, 6), date.substring(6, 8))
    }else{
      (date.substring(0, 4), date.substring(5, 7), date.substring(8, 10))
    }
  }


  def getNowUTC: LocalDateTime = {
    LocalDateTime.now(ZoneId.of("UTC+0"))
  }

  def today:LocalDate = {
    LocalDate.now(ZoneId.of("UTC+0"))
  }

  def getDateTimeInDMSFormat(dateTime: LocalDateTime): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.nnnnnnnnn")
    dateTime.format(formatter).dropRight(3)
  }

  def getNowInDMSFormat: String = {
    getDateTimeInDMSFormat(getNowUTC)
  }

  def dateStringToS3Path(date:String): String = {
    val (year, month, day) = splitDate(date)
    s"dl_year=$year/dl_month=$month/dl_day=$day/"
  }

  def getDate(dateStr:String):LocalDate = {
    val pattern = if(dateStr.length == 8){"yyyyMMdd"}else{"yyyy-MM-dd"}
    val formatter = DateTimeFormatter.ofPattern(pattern)
    LocalDate.parse(dateStr, formatter)
  }

  def getReadableTimestamp(d: Date): String = {
    val df = new SimpleDateFormat("yyyyMMdd-HHmmss")
    df.setTimeZone(TimeZone.getTimeZone("UTC"))
    df.format(d)
  }

}
