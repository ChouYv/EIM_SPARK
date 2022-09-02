package org.sanofi.utils

import org.apache.spark.sql.SparkSession

object sparkUtils {
    def getOrCreateSparkSession(master:String,appName:String):SparkSession ={
        val spark: SparkSession = SparkSession
          .builder()
          .appName(appName)
          .master(master)
          .enableHiveSupport()
          .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        spark
    }
}
