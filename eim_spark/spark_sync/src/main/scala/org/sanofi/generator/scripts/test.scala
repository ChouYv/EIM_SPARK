package org.sanofi.generator.scripts

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object test {
  def main(args: Array[String]): Unit = {

    val bdmpPgDatabases: String = sys.env("BDMP_PG_DATABASES")
    val bdmpPgUrl: String = sys.env("BDMP_PG_URL")
    val bdmpPgPort: String = sys.env("BDMP_PG_PORT")
    val bdmpPgUser: String = sys.env("BDMP_PG_USER")
    val bdmpPgPwd: String = sys.env("BDMP_PG_PWD")
    val cosPath: String = sys.env("BDMP_SOURCE_FILE_COS_PATH")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("generateScripts")
      .master("local[4]")
      //      .master("yarn")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
//    val inputFileName = "MD_CL_CONTENT_VIDEO_MAP"
        val inputFileName = "MD_CL_CONTENT"
//        val inputFileName = "MD_CL_MENU"
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

    if (1.toLong != l) println(s"传入参数inputFileName为${inputFileName}----->对应不止一张表")

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

    val loadKeyList: Array[String] = loadKey.toLowerCase.split(",")

    println(loadKeyList.mkString(","))
    println(loadKey)
    //    获取列的相关信息
    val sql2: String =
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
    val pkAndBkListDF: DataFrame = baseDF.selectExpr(
      "concat_ws (',', collect_list(if(primary_key='Y',lower(coalesce(if(field_alias='',null,field_alias),name)),null))) as pk_list",
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
    println("\nldg==========================\n")
    val ldgDDLPrd: String = s"CREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName} (\n"
    val ldgDDLMid: String = midDF.map(
      line => "\t`" + line.getAs("field_name").toString + "` string"
    ).collect()
      .mkString(",\n")
    val ldgDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}_${ddlName}' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"

    val ldgDDLSql: String = ldgDDLPrd + ldgDDLMid + ldgDDLSuf
    println(ldgDDLSql)


    //    ldg-pk
    println("\nldgpk==========================\n")

    val ldgPkDDLPrd: String = s"CREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName}_pk (\n"
    val ldgPkDDLMid: String =
      pkAndBkListDF.selectExpr("coalesce(if(bk_list='',null,bk_list),pk_list) as pk")
        .map(
          row => row.getAs("pk")
            .toString
            .split(",")
            .map("\t" + _ + " string")
            .mkString(",\n")
        ).first()
    val ldgPkDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}_${ddlName}_pk' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"

    val ldgPkDDLSql = ldgPkDDLPrd + ldgPkDDLMid + ldgPkDDLSuf
    println(ldgPkDDLSql)


    //    stg
    println("\nstg==========================\n")

    val stgDDLPrd: String = s"CREATE TABLE IF NOT EXISTS stg.${ddlName} (\n"
    val stgDDLMid: String = midDF.map(
      line => "\t`" + line.getAs("field_name").toString + "` string comment '" + line.getAs("field_comment") + "'"
    ).collect()
      .mkString(",\n")
    val stgDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val stgDDLSql = stgDDLPrd + stgDDLMid + stgDDLSuf
    println(stgDDLSql)


    //    stg_pk
    println("\nstgpk==========================\n")

    val stgPkDDLPrd: String = s"CREATE TABLE IF NOT EXISTS stg.${ddlName}_pk (\n"
    val stgPkDDLMid: String = ldgPkDDLMid
    val stgPkDDLSuf: String = stgDDLSuf

    val stgPkDDLSql = stgPkDDLPrd + stgPkDDLMid + stgPkDDLSuf
    println(stgPkDDLSql)


    //    stg_rej
    println("\nstgrej==========================\n")

    val rejDDLPrd: String = s"CREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_rej (\n"
    val rejDDLMid: String = stgDDLMid + ",\n\t`flag` string comment '标签'"
    val rejDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val rejDDLSql = rejDDLPrd + rejDDLMid + rejDDLSuf
    println(rejDDLSql)


    //    rej_pk
    println("\nrejpk==========================\n")

    val rejPkDDLPrd: String = s"CREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_pk (\n"
    val rejPkDDLMid: String = ldgPkDDLMid + ",\n\t`flag` string "
    val rejPkDDLSuf: String = stgDDLSuf

    val rejPkDDLSql = rejPkDDLPrd + rejPkDDLMid + rejPkDDLSuf
    println(rejPkDDLSql)


    //    ods
    println("\nods==========================\n")

    var partKey: String = ""
    if ("full delete full load by key" == loadSolution) {
      partKey = "loadKey"
    } else if ("Full" == fullDelta & "full delete full load by key" != loadSolution) {
      partKey = ""
    } else if ("Delta" == fullDelta & "full delete full load by key" != loadSolution) {
      partKey = "eim_dt"
    }
    val odsPkDDLPrd: String = s"CREATE TABLE IF NOT EXISTS ods.${ddlName} (\n"
    val odsPkDDLMidOne: String = midDF.filter(
      i => {
        if (partKey == "loadKey" & loadKeyList.contains(i.getAs("field_name").toString)) false else true
      }
    ).map(
      line => {
        "\t`" + line.getAs("field_name").toString + "` string comment '" + line.getAs("field_comment") + "'"
      }
    ).collect()
      .mkString(",\n")

    val odsPkDDLMidTwo: String = ",\n\t`etl_is_hard_del` int COMMENT '0,1'," +
      "\n\t`etl_created_ts` timestamp COMMENT 'eim_创建时间'," +
      "\n\t`etl_modified_ts` timestamp COMMENT 'eim_修改时间'" + (
      if ("eim_dt" != partKey) ",\n\t`eim_dt` string" else ""
      )

    val odsPkDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \n" +
      (
        partKey match {
          case "loadKey" => "PARTITIONED BY (" + loadKeyList.map(i => "`" + i + "` string").mkString(",") + ") \n"
          case "eim_dt" => "PARTITIONED BY (`eim_dt` string) \n"
          case _ => ""
        }
        ) +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val odsPkDDLSql = odsPkDDLPrd + odsPkDDLMidOne + odsPkDDLMidTwo + odsPkDDLSuf
    println(odsPkDDLSql)


    spark.stop()

  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }


  def removeSpecialChar(input: String): String = {
    "asda"
  }

}
