package org.sanofi

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("hive test")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("show databases;").show()
    sql("show tables").show()
    sql("create table if not exists test001(id int,name string)")
    sql("insert into table test001 values(1,'zhouyahui')")
    sql("insert into table test001 values(2,'zhouyahui2')")
    sql("show create table test001 ").show(100)
    spark.stop()

  }
}
