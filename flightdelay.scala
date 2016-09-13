package com.cloudera

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zengxiaosen on 16/8/24.
  */
object flightdelay {

  case class Flight(dofM: String, dofW: String, carrier: String, tailnum: String, flnum: Int, org_id: String, origin: String, dest_id: String, dest: String, crsdeptime: Double, deptime: Double, depdelaymins: Double, crsarrtime: Double, arrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    //function to parse input into Movie class

    def parseFlight(str: String): Flight = {
      val line = str.split(",")
      Flight(line(0), line(1), line(2), line(3), line(4).toInt, line(5), line(6), line(7), line(8), line(9).toDouble, line(10).toDouble, line(11).toDouble, line(12).toDouble, line(13).toDouble, line(14).toDouble, line(15).toDouble, line(16).toInt)
    }

    //mllib
    val textRDD = sc.textFile("/Users/zengxiaosen/Documents/data/rita2014jan.csv")

    val flightsRDD = textRDD.map(parseFlight).cache()
    val flightsDF = flightsRDD.toDF()

    flightsDF.registerTempTable("flights")

    //enter to sparksql

    import org.apache.spark.sql.functions._

    //creating DF
    //number of departing flights
    val fltCountsql = sqlContext.sql("select dofM, dest, count(flnum) as total FROM flights GROUP BY dofM, dest ORDER BY total DESC LIMIT 5")

    //find average delay
    val avgdepdel = sqlContext.sql("SELECT origin, avg(depdelaymins) as avgdepdelay FROM flights GROUP BY origin ORDER BY avgdepdelay DESC LIMIT 5")

    println(s"------------------------------------------------------------------------------------")
    println(s"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% SPARK SQL - AVERAGE DELAY AT ORIGIN %%%%%%%%%%%%%%%%%%%")
    println(s"-------------------------------------------------------------------------------------------")

    avgdepdel.show()
    fltCountsql.show()

  }
}
