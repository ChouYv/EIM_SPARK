package org.sanofi

import org.apache.spark.sql.{DataFrame, SparkSession}


object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("generateScripts")
      .master("yarn")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val inputFileName = "MD_CL_CONTENT"
    val BDMPPostgresUrl = "jdbc:postgresql://localhost:55432/postgres"
    val BDMPPostgresUser = "postgres"
    val BDMPPostgresPwd = "rootroot"


    val connOptionsMap =
      Map("url" -> BDMPPostgresUrl,
        "user" -> BDMPPostgresUser,
        "password" -> BDMPPostgresPwd)

    val bdmpInterfaceDf = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface")
    bdmpInterfaceDf.createOrReplaceTempView("PgInterface")

    val bdmpInterfaceFieldDf = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_field")
    bdmpInterfaceFieldDf.createOrReplaceTempView("PgField")

    val bdmpInterfaceFileDf = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_file")
    bdmpInterfaceFileDf.createOrReplaceTempView("PgFile")

    val bdmpSqlMapDf = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_sql_map")
    bdmpSqlMapDf.createOrReplaceTempView("PgSqlMap")


    val sql1 =
      """select
        |    bi.id as bi_id,
        |    bif.id as bif_id,
        |    bif.tab_comment_cn,
        |    bif.load_solution,
        |    bif.full_delta,
        |    bif.if_physical_deletion,
        |    lower(bi.source_system)||'_'||lower(coalesce(bif.file_alias,bif.name)) as ddl_name
        |from PgFile bif
        |         left join PgInterface bi on bi.id = bif.interface_id
        |where bi.status=3 and bi.deleted=false and  bif.file_alias = '""".stripMargin + inputFileName + "';"

    println(sql1)
    spark.sql(sql1).show()
    spark.stop()

  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }


}
