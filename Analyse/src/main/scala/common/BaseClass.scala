package com.sharing.utils

import com.sharing.Params
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by xia.jun on 16/8/8.
 */
trait BaseClass {
  /**
   * define some parameters
   */
  var sc:SparkContext = null
  implicit var sqlContext:SQLContext = null
  val config = new SparkConf()
  var spark:SparkSession = null


  /**
   * initialize global parameters
   */
  def init()={
    spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
    }


  /**
   * this method do not complete.Sub class that extends BaseClass complete this method
   */
  def execute(params: Params)


  /**
   * release resource
   */
  def destroy()={
    if(sc!=null) {
      sqlContext.clearCache()
      sc.stop()
    }
  }

}
