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
      .option("header","true")                               //to get the column names//
      .option("inferSchema","true")                           //insert the schema//
      .csv("C:/Users/user/IdeaProjects/FifaDataAnalysis/sourcedata/fifa2019.csv")
    val firstDf=dataDF.select("_c0","ID","Name","Age","Nationality","Overall","Club","Value","Wage")
    firstDf.drop(col("_c0")).show()                                   // droping the column//
   println(dataDF.columns.toList)                                             //to make the list of column//
   dataDF.select(col("ID"),col("Name"), col("Age")).show()
    logger.info("fifa data has loaded")
//======================this is practised on 28/07/2020 ===========================================//

    val secondDf=firstDf.withColumn("Name",regexp_replace(col("Name")," ","").alias("Name"))
        .withColumn("Club",regexp_replace(col("Club")," ","").alias("Club"))
        .withColumn("Value",regexp_replace(col("Value"),"€","").alias("Value"))
        .withColumn("Wage",regexp_replace(col("Wage"),"€",""))
        .withColumn("Value",when(col("Value").endsWith("M"),regexp_replace(col("Value"),"M","")*1000000).alias("Value"))
//        .withColumn("wage",when(col("Wage").endsWith("K"),regexp_replace(col("Value"),"K","")*1000).alias("Wage"))
        .withColumn("Wage",regexp_extract(col("Wage"),"\\d+",0)*1000)
         secondDf.show()


  }

}
