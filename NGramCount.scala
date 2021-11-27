package com.javadeveloperzone.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._

object NGramCount {

	def main(args: Array[String]) {

        /*By default we are setting local so it will run locally with one thread 
         *Specify: "local[2]" to run locally with 2 cores, OR 
         *        "spark://master:7077" to run on a Spark standalone cluster */

		
		val sparkContext = new SparkContext("local","Spark NGramCount example using Scala",
		System.getenv("SPARK_HOME"))
		//val sparkContext = sc

		/*Reading input from File*/
		val input = sparkContext.textFile(args(2))

		/*Creating RDD from lines on input file*/
		val ngrams = input.flatMap(line => line.split(" ").sliding(args(0).toInt))

		/*Performing mapping and reducing operation*/
		val counts = ngrams.map(ngram => (ngram.mkString(" "), 1)).reduceByKey{case (x, y) => x + y}.filter{case (x, y) => y >= args(1).toInt}

		/*Saving the result file to the location that we have specified as args[3]*/
		counts.saveAsTextFile(args(3))

	}
}
