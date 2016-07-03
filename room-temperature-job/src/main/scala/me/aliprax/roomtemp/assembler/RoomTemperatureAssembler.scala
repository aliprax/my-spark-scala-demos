package me.aliprax.roomtemp.assembler

import me.aliprax.roomtemp.entities.RoomTemperature
import org.apache.spark.rdd.RDD
import scala._

class RoomTemperatureAssembler(val roomIdPosition: Int, val temperaturePosition: Int) 
  extends java.io.Serializable{
  
  def createRdd(lines: RDD[String]):RDD[RoomTemperature] = {
     val linesAsArrayOfStrings = lines.map(line => line.split(","))
     linesAsArrayOfStrings.flatMap { x =>
       try{
         val temperature = x(temperaturePosition).toDouble
         val roomId = x(roomIdPosition).toLong
          Some(new RoomTemperature(roomId,temperature))
       }catch {
         case _: Throwable => None
       }
     }
  }
  
}