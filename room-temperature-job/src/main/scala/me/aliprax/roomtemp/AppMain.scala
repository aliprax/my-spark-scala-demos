package me.aliprax.roomtemp

import org.apache.spark._
import me.aliprax.roomtemp.readers.FakeCsvReader
import me.aliprax.roomtemp.assembler.RoomTemperatureAssembler
import me.aliprax.roomtemp.tasks.RoomTemperatureAverage

/**
 * @author ${user.name}
 */
object AppMain {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleJob").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = FakeCsvReader.readData(sc);
    val samples = new RoomTemperatureAssembler(0,1).createRdd(lines);
    val roomTemperatureMeans = RoomTemperatureAverage.compute(samples);
    println(roomTemperatureMeans.foreach(println _))
  }
  
}
