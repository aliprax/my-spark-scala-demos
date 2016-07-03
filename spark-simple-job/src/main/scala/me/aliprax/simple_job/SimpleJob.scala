package me.aliprax.simple_job

import org.apache.spark._

/**
 * @author ${user.name}
 */
object SimpleJob {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimpleJob").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = sc.parallelize(3 to 5000)
    val primes = numbers.filter(isPrime(_));
    println("Number of primes: " + primes.count())
  }

  def isPrime(x: Int): Boolean = {
    val numbers = (2 to (x - 1) toList)
    numbers.map(test => x % test).forall(_ != 0)
  }

}
