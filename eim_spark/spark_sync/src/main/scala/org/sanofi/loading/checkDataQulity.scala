package org.sanofi.loading

import org.apache.derby.impl.sql.compile.TableName
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class checkDataQulity (spark: SparkSession,df: DataFrame,tablesMap: mutable.HashMap[String, mutable.Map[String, String]]){



  def dq(tableName: String):Unit ={


  }
}
