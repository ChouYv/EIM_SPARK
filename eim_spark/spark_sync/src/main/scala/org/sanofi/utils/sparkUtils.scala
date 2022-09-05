package org.sanofi.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object sparkUtils extends Serializable {
    def getOrCreateSparkSession(master:String,appName:String):SparkSession ={
        val spark: SparkSession = SparkSession
          .builder()
          .appName(appName)
          .master(master)
          .enableHiveSupport()
          .config(new SparkConf().setJars( Seq{"/home/zhouyahui/Projects/EIM_SPARK/eim_spark/spark_sync/target/spark_sync-1.0-SNAPSHOT.jar"}))
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        spark
    }
}
