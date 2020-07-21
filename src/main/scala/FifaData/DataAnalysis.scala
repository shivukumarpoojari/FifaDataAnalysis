package FifaData


import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import config.getSparkConf

object DataAnalysis {
  @transient lazy val logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    logger.info("creating spark session")
    val spark=SparkSession.builder().config(getSparkConf).getOrCreate()
    import spark.implicits
    logger.info("spark session has been created")
    logger.info("loading fifa dataset on the year of 2019")
    val dataDF=spark
      .read
      .option("header","true")                               
      .option("inferSchema","true")                        
      .csv("C:/Users/user/IdeaProjects/FifaDataAnalysis/sourcedata/fifa2019.csv")
    val firstDf=dataDF.select("_c0","ID","Name","Age","Nationality","Overall","Club","Value","Wage")
    firstDf.drop(col("_c0")).show()                                  
    println(dataDF.columns.toList)                                            
    dataDF.select("_c0","_c1","_c2","_c3","_c4","_c5","_c6").show()
    dataDF.select(col("ID"),col("Name"), col("Age")).show()
    logger.info("fifa data has loaded")

  }

}
