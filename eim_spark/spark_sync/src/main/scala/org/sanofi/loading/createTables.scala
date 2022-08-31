package org.sanofi.loading

import org.apache.spark.sql.{DataFrame, SparkSession}

class createTables(spark: SparkSession, envArgMap: Map[String, String]) {

  val bdmpPgDatabases: String = envArgMap("BDMP_PG_DATABASES")
  val bdmpPgUrl: String = envArgMap("BDMP_PG_URL")
  val bdmpPgPort: String = envArgMap("BDMP_PG_PORT")
  val bdmpPgUser: String = envArgMap("BDMP_PG_USER")
  val bdmpPgPwd: String = envArgMap("BDMP_PG_PWD")
  val cosPath: String = envArgMap("BDMP_SOURCE_FILE_COS_PATH")


  val connOptionsMap: Map[String, String] =
    Map("url" -> s"jdbc:postgresql://${bdmpPgUrl}:${bdmpPgPort}/${bdmpPgDatabases}",
      "user" -> bdmpPgUser,
      "password" -> bdmpPgPwd)

  val bdmpInterfaceDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface")
  bdmpInterfaceDf.createOrReplaceTempView("PgInterface")

  val bdmpInterfaceFieldDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_field")
  bdmpInterfaceFieldDf.createOrReplaceTempView("PgField")

  val bdmpInterfaceFileDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_file")
  bdmpInterfaceFileDf.createOrReplaceTempView("PgFile")

  val bdmpSqlMapDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_sql_map")
  bdmpSqlMapDf.createOrReplaceTempView("PgSqlMap")

  def getldgDDLSql(): String = {
    ""
  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }
}
