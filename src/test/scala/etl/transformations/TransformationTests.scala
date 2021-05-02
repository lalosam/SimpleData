package org.rojosam
package etl.transformations

import java.time.{LocalDateTime, ZoneId}
import com.holdenkarau.spark.testing.{Column, DataFrameSuiteBase, DataframeGenerator}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop.propBoolean
import org.apache.spark.sql._
import etl.UnitSpecSpark
import etl.utils.DateTimeUtils

class TransformationTests extends UnitSpecSpark with DataFrameSuiteBase {



  "AddDateColumns Transformation" should "add columns dl_year, dl_month and dl_day" in {
    val sqlCtx = sqlContext

    val schema = StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true)
    ))

    val dateGenerator = new Column("timestamp", Gen.oneOf("2019-09-12", "2019-08-11", "2019-12-31"))
    val numberGenerator = new Column("number", Gen.choose(10, 100))
    val genInput = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlCtx, schema)(dateGenerator, numberGenerator)
    val property =
      Prop.forAll(genInput.arbitrary) {
        input =>
          val output = Transformation(input).addDateColumns("timestamp").getDF
          (input.columns.length + 3 === output.columns.length) :| "Incorrect number of columns" &&
            output.columns.contains("dl_year") :| "[dl_year] column is not present" &&
            output.columns.contains("dl_month") :| "[dl_month] column is not present" &&
            output.columns.contains("dl_day") :| "[dl_day] column is not present" &&
            output.schema("dl_year").dataType.equals(StringType) :| "[dl_year] column is not StringType"
      }
    check(property)
  }

  "AddTimestamp" should "add Timestamp column with current time" in {
    val sqlCtx = sqlContext

    val schema = StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true)
    ))

    val dateGenerator = new Column("timestamp", Gen.oneOf("2019-09-12", "2019-08-11", "2019-12-31"))
    val numberGenerator = new Column("number", Gen.choose(10, 100))
    val genInput = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlCtx, schema)(dateGenerator, numberGenerator)
    val property =
      Prop.forAll(genInput.arbitrary) {
        input =>
          val output = Transformation(input).addTimestamp("event_timestamp").getDF
          (input.columns.length + 1 === output.columns.length) :| "Incorrect number of columns" &&
            output.columns.contains("event_timestamp") :| "[event_timestamp] column is not present"
      }
    check(property)
  }

  it should "add the specified Timestamp column" in {
    val sqlCtx = sqlContext

    val schema = StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true)
    ))

    val dateGenerator = new Column("timestamp", Gen.oneOf("2019-09-12", "2019-08-11", "2019-12-31"))
    val numberGenerator = new Column("number", Gen.choose(10, 100))
    val genInput = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlCtx, schema)(dateGenerator, numberGenerator)
    val property =
      Prop.forAll(genInput.arbitrary) {
        input =>
          val localTime = LocalDateTime.now(ZoneId.of("UTC+0"))
          val expectedTimestamp = DateTimeUtils.getDateTimeInDMSFormat(localTime)
          val output = Transformation(input).addTimestamp("event_timestamp", localTime).getDF
          output.show()
          val gottenTimestamp = if(output.count() > 0){
            Some(output.take(1)(0).getString(2))
          }else{
            None
          }
          println(s"EXPECTED TIMESTAMP: [$expectedTimestamp] ----> GOTTEN TIMESTAMP: [${gottenTimestamp.getOrElse("")}]")
          (input.columns.length + 1 === output.columns.length) :| "Incorrect number of columns" &&
            output.columns.contains("event_timestamp") :| "[event_timestamp] column is not present" &&
            (gottenTimestamp.isEmpty || gottenTimestamp.get === expectedTimestamp ) :| s"Timestamp is not being generated properly, expected[$expectedTimestamp], gotten [$gottenTimestamp]"
      }
    check(property)
  }

  "DropDateColumns Transformation" should "remove columns dl_year, dl_month and dl_day" in {
    val sqlCtx = sqlContext

    val schema = StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true),
      StructField("dl_year", StringType, nullable = true),
      StructField("dl_month", StringType, nullable = true),
      StructField("dl_day", StringType, nullable = true)
    ))

    val dateGenerator = new Column("timestamp", Gen.oneOf("2019-09-12", "2019-08-11", "2019-12-31"))
    val numberGenerator = new Column("number", Gen.choose(10, 100))
    val dlYearGenerator = new Column("dl_year", Gen.oneOf("2017", "2018", "2019"))
    val dlMonthGenerator = new Column("dl_month", Gen.oneOf("01","02", "03"))
    val dlDayGenerator = new Column("dl_day", Gen.oneOf("01","02", "03"))
    val genInput = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlCtx, schema)(dateGenerator,
      numberGenerator, dlYearGenerator, dlMonthGenerator, dlDayGenerator)
    val property =
      Prop.forAll(genInput.arbitrary) {
        input =>
          val output = Transformation(input).dropDateColumns().getDF
          (input.columns.length - 3 === output.columns.length) :| "Incorrect number of columns" &&
            !output.columns.contains("dl_year") :| "[dl_year] column is still present" &&
            !output.columns.contains("dl_month") :| "[dl_month] column is still present" &&
            !output.columns.contains("dl_day") :| "[dl_day] column is still present"
      }
    check(property)
  }

  "PrependColumn Transformation" should "prepend a column" in {
    val sqlCtx = sqlContext

    val schema = StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("number", IntegerType, nullable = true)
    ))

    val dateGenerator = new Column("timestamp", Gen.oneOf("2019-09-12", "2019-08-11", "2019-12-31"))
    val numberGenerator = new Column("number", Gen.choose(10, 100))
    val genInput = DataframeGenerator.arbitraryDataFrameWithCustomFields(sqlCtx, schema)(dateGenerator, numberGenerator)
    val property =
      Prop.forAll(genInput.arbitrary) {
        input =>
          val output = Transformation(input).prependColumn("new", "anyvalue").getDF
          (input.columns.length + 1 === output.columns.length) :| "Incorrect number of columns" &&
            (output.columns(0) === "new") :| "The first column is not the prepend column" &&
            (output.columns.tail === input.columns) :| "The remainig columns are not the same"
      }
    check(property)
  }



}

