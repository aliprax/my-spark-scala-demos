package me.aliprax.roomtemp.readers

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.util.Random

object FakeCsvReader {
  
  def readData(sc: SparkContext): RDD[String] = {
    val roomIds = 1 to 10
    val numberOfMeasures = 100000
    val roomIdsMeasures = roomIds.map(id => (id,1 to numberOfMeasures map(_ => Random.nextGaussian()*4+20 )))
    val stringsAsCsvFile = roomIdsMeasures.map( x => ""+x._1+","+x._2.mkString(",") )
    sc.parallelize(stringsAsCsvFile)
  }
  
}