package org.sanofi.utils

import org.apache.spark.sql.SparkSession

object sparkUtils {
    def getOrCreateSparkSession(master:String,appName:String):SparkSession ={
        SparkSession
          .builder()
          .appName(appName)
          .master(master)
          .getOrCreate()
    }
}
