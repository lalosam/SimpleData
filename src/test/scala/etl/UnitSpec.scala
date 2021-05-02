package org.rojosam
package etl

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Inside, Inspectors, Matchers, OptionValues}
import org.scalatestplus.scalacheck.Checkers

abstract class UnitSpec extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with Checkers
  with BeforeAndAfter {

}
