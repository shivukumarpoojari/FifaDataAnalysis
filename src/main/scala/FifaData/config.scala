package FifaData

import java.util.Properties

import org.apache.spark.SparkConf

import scala.io.Source


object config {
  def getSparkConf:SparkConf={
    val conf=new SparkConf()
    val props=new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k,v)=>conf.set(k.toString,v.toString))
    conf
  }

}
