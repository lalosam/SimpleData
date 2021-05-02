package org.rojosam
package etl

import com.holdenkarau.spark.testing.SharedSparkContext

class UnitSpecSpark extends UnitSpec with SharedSparkContext{

  self: SharedSparkContext =>

  before{
    println("\n\n** Starting Spark test\n")
  }

}