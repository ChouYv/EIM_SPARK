package org.sanofi

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object test {
  def main(args: Array[String]): Unit = {

    val bdmpPgDatabases: String = sys.env.get("BDMP_PG_DATABASES").get
    val bdmpPgUrl: String = sys.env.get("BDMP_PG_URL").get
    val bdmpPgPort: String = sys.env.get("BDMP_PG_PORT").get
    val bdmpPgUser: String = sys.env.get("BDMP_PG_USER").get
    val bdmpPgPwd: String = sys.env.get("BDMP_PG_PWD").get
    val cosPath: String = sys.env.get("BDMP_SOURCE_FILE_COS_PATH").get

    val spark: SparkSession = SparkSession
      .builder()
      .appName("generateScripts")
      .master("local[4]")
//      .master("yarn")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val inputFileName = "MD_CL_CONTENT"
    val BDMPPostgresUrl = s"jdbc:postgresql://${bdmpPgUrl}:${bdmpPgPort}/${bdmpPgDatabases}"
    val BDMPPostgresUser = bdmpPgUser
    val BDMPPostgresPwd = bdmpPgPwd


    val connOptionsMap: Map[String, String] =
      Map("url" -> BDMPPostgresUrl,
        "user" -> BDMPPostgresUser,
        "password" -> BDMPPostgresPwd)

    val bdmpInterfaceDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface")
    bdmpInterfaceDf.createOrReplaceTempView("PgInterface")

    val bdmpInterfaceFieldDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_field")
    bdmpInterfaceFieldDf.createOrReplaceTempView("PgField")

    val bdmpInterfaceFileDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_interface_file")
    bdmpInterfaceFileDf.createOrReplaceTempView("PgFile")

    val bdmpSqlMapDf: DataFrame = getPostgresDf(spark, connOptionsMap, "app_upt.bdmp_sql_map")
    bdmpSqlMapDf.createOrReplaceTempView("PgSqlMap")

//   获取表名等基本信息
    val sql1: String =
      """select
        |    bi.id as bi_id,
        |    bi.source_system as source_system,
        |    bif.id as bif_id,
        |    bif.tab_comment_cn,
        |    bif.load_solution,
        |    bif.full_delta,
        |    bif.if_physical_deletion,
        |    bif.load_key,
        |    lower(bi.source_system)||'_'||lower(coalesce(bif.file_alias,bif.name)) as ddl_name
        |from PgFile bif
        |         left join PgInterface bi on bi.id = bif.interface_id
        |where bi.status=3 and bi.deleted=false and  coalesce(bif.file_alias,bif.name) = '""".stripMargin + inputFileName + "';"
    println(sql1)
// 注意顺序
    val l: Long = spark.sql(sql1).count()

    if (1.toLong != l ) println(s"传入参数inputFileName为${inputFileName}----->对应不止一张表")

    val df1Row: Row = spark.sql(sql1).first()

    val pgInterfaceId = df1Row.getAs("bi_id").toString
    val sourceSystem = df1Row.getAs("source_system").toString
    val pgFileId = df1Row.getAs("bif_id").toString
    val tabCommentCn = df1Row.getAs("tab_comment_cn").toString
    val loadSolution = df1Row.getAs("load_solution").toString
    val fullDelta = df1Row.getAs("full_delta").toString
    val ifPhysicalDeletion = df1Row.getAs("if_physical_deletion").toString
    val loadKey = df1Row.getAs("load_key").toString
    val ddlName = df1Row.getAs("ddl_name").toString

//    获取列的相关信息
    val sql2 :String =
      """
        |select name,
        |       field_alias,
        |       field_type,
        |       value_range,
        |       index,
        |       primary_key,
        |       business_key,
        |       fields_comment_en,
        |       fields_comment_cn,
        |       if_enum_field,
        |       if_not_null
        |from PgField where file_id= """.stripMargin + pgFileId + " order by index;"
    println(sql2)
    val baseDF: DataFrame = spark.sql(sql2).orderBy("index")
    baseDF.show()
    val pkAndBkListDF: DataFrame = baseDF.selectExpr("concat_ws (',', collect_list(if(primary_key='Y',lower(coalesce(if(field_alias='',null,field_alias),name)),null))) as pk_list",
      "concat_ws (',', collect_list(if(business_key='Y',lower(coalesce(if(field_alias='',null,field_alias),name)),null))) as bk_list")
    pkAndBkListDF.show()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val midDF: DataFrame = baseDF
      .selectExpr(
        "lower(coalesce(if(field_alias='',null,field_alias),name)) as field_name"
        , "lower(field_type) as field_type"
        , "regexp_replace(fields_comment_en||'||'||fields_comment_cn,'[.,`''\\/{}$]',' ') as field_comment")
      .orderBy("index")

//       ldg
    val ldgDDLPrd:String =s"CREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName} (\n"
    val ldgDDLMid: String = midDF.map((line: Row) => "\t`" + line.getAs("field_name").toString + "` string comment '" + line.getAs("field_comment") + "'")
      .collect().mkString(",\n")
    val ldgDDLSuf:String =s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}_${ddlName}' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"




    val ldgDDLSql: String =ldgDDLPrd+ldgDDLMid+ldgDDLSuf
    println(ldgDDLSql)

//    ldg-pk
    val ldgPkDDLPrd:String =s"CREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName}_pk (\n"
    val ldgPkDDLMid: String = pkAndBkListDF.selectExpr("coalesce(if(bk_list='',null,bk_list),pk_list) as pk").map(
      (row: Row) => row.getAs("pk").toString.split(",").map("\t" + _ + " string").mkString(",\n")
    ).first()
    val ldgPkDDLSuf:String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}_${ddlName}_pk' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"



    val ldgPkDDLSql=ldgPkDDLPrd+ldgPkDDLMid+ldgPkDDLSuf
    println(ldgPkDDLSql)






    spark.stop()

  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }



  def removeSpecialChar(input: String):String = {
    "asda"
  }

}
