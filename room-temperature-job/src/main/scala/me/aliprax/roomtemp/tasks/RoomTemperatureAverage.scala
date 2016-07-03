package me.aliprax.roomtemp.tasks

import org.apache.spark.rdd.RDD
import me.aliprax.roomtemp.entities.RoomTemperature
import org.apache.spark.SparkContext._

object RoomTemperatureAverage {
  
  def compute(roomsTemperature: RDD[RoomTemperature]) = {
    val keyValuePairs = roomsTemperature.map(sample => (sample.roomId, ( sample.temperature,1) ))
    keyValuePairs.
      reduceByKey { (x,y) =>
        val (xtemperature,xcount) = x
        val (ytemperature,ycount) = y
        (xtemperature+ytemperature,xcount+ycount)
      }. 
      mapValues { v =>
        val (sumOfTemperatures,measuerementCount) = v
        sumOfTemperatures / measuerementCount
      }
  }
}