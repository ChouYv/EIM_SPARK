package org.sanofi.loading

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import java.io._


class createTables(spark: SparkSession, envArgMap: Map[String, String], argsMap: mutable.Map[String, String]) extends Serializable {

  val DDLCreateSqlMap = new mutable.HashMap[String, String]
  val fileNameAndTableNameMap = new mutable.HashMap[String, mutable.Map[String, String]]

  val bdmpPgDatabases: String = envArgMap("BDMP_PG_DATABASES")
  val bdmpPgUrl: String = envArgMap("BDMP_PG_URL")
  val bdmpPgPort: String = envArgMap("BDMP_PG_PORT")
  val bdmpPgUser: String = envArgMap("BDMP_PG_USER")
  val bdmpPgPwd: String = envArgMap("BDMP_PG_PWD")
  val cosPath: String = envArgMap("BDMP_SOURCE_FILE_COS_PATH")

  private val tableList: Array[String] = argsMap("tableList").split(",")

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


  private var DQDF: DataFrame = spark.sql(
    """
      |       select
      |       '' as file_name,
      |       name,
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
      |     from PgField where 1=0
      """.stripMargin)


  for (elem <- tableList) {
    getDDLSql(elem, spark)
  }


  def getDDLSql(elem: String, spark: SparkSession): Unit = {
    val inputFileName = elem
    val sql1: String =
      """
        |   select
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
    val baseDF: DataFrame = spark.sql(sql2).orderBy("index")
    DQDF= DQDF.unionAll(baseDF.selectExpr(s"lower('${inputFileName}') as file_name", "*"))
    val pkAndBkListDF: DataFrame = baseDF.selectExpr(
      "concat_ws (',', collect_list(if(primary_key='Y',lower(coalesce(if(field_alias='',null,field_alias),name)),null))) as pk_list",
      "concat_ws (',', collect_list(if(business_key='Y',lower(coalesce(if(field_alias='',null,field_alias),name)),null))) as bk_list")

    import spark.implicits._
    val midDF: DataFrame = baseDF
      .selectExpr(
        "lower(coalesce(if(field_alias='',null,field_alias),name)) as field_name"
        , "lower(field_type) as field_type"
        , "regexp_replace(fields_comment_en||'||'||fields_comment_cn,'[.,`''\\/{}$]',' ') as field_comment")
      .orderBy("index")


    //       ldg
    val ldgDDLPrd: String = s"drop table if exists ldg.${ddlName};\nCREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName} (\n"
    val ldgDDLMid: String = midDF.map(
      line => "\t`" + line.getAs("field_name").toString + "` string"
    ).collect()
      .mkString(",\n")
    val ldgDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}/${ddlName}' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"

    val ldgDDLSql: String = ldgDDLPrd + ldgDDLMid + ldgDDLSuf


    DDLCreateSqlMap += (s"${inputFileName}" -> ldgDDLSql)

    //    ldg-pk
    val ldgPkDDLPrd: String = s"drop table if exists ldg.${ddlName}_pk;\nCREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName}_pk (\n"
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
      s"LOCATION 'cosn://${cosPath}/${sourceSystem}${ddlName}_pk' \n" +
      s"TBLPROPERTIES ('skip.header.line.count'='1');"


    val ldgPkDDLSql = ldgPkDDLPrd + ldgPkDDLMid + ldgPkDDLSuf
    if ("Y" == ifPhysicalDeletion) DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + ldgPkDDLSql


    //    stg
    val stgDDLPrd: String = s"drop table if exists stg.${ddlName};\nCREATE TABLE IF NOT EXISTS stg.${ddlName} (\n"
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
    DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + stgDDLSql


    //    stg_pk
    val stgPkDDLPrd: String = s"drop table if exists stg.${ddlName}_pk;\nCREATE TABLE IF NOT EXISTS stg.${ddlName}_pk (\n"
    val stgPkDDLMid: String = ldgPkDDLMid
    val stgPkDDLSuf: String = stgDDLSuf

    val stgPkDDLSql = stgPkDDLPrd + stgPkDDLMid + stgPkDDLSuf
    if ("Y" == ifPhysicalDeletion) DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + stgPkDDLSql


    //    stg_rej
    val rejDDLPrd: String = s"drop table if exists rej.ldg_${ddlName}_rej;\nCREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_rej (\n"
    val rejDDLMid: String = stgDDLMid + ",\n\t`flag` string comment '标签'"
    val rejDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val rejDDLSql = rejDDLPrd + rejDDLMid + rejDDLSuf
    DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + rejDDLSql


    //    rej_pk
    val rejPkDDLPrd: String = s"drop table if exists rej.ldg_${ddlName}_pk;\nCREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_pk (\n"
    val rejPkDDLMid: String = ldgPkDDLMid + ",\n\t`flag` string "
    val rejPkDDLSuf: String = stgDDLSuf

    val rejPkDDLSql = rejPkDDLPrd + rejPkDDLMid + rejPkDDLSuf
    if ("Y" == ifPhysicalDeletion) DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + rejPkDDLSql


    //    ods
    var partKey: String = ""
    if ("full delete full load by key" == loadSolution) {
      partKey = "loadKey"
    } else if ("Full" == fullDelta & "full delete full load by key" != loadSolution) {
      partKey = ""
    } else if ("Delta" == fullDelta & "full delete full load by key" != loadSolution) {
      partKey = "eim_dt"
    }
    val odsDDLPrd: String = s"drop table if exists ods.${ddlName};\nCREATE TABLE IF NOT EXISTS ods.${ddlName} (\n"
    val odsDDLMidOne: String = midDF.filter(
      i => {
        if (partKey == "loadKey" & loadKeyList.contains(i.getAs("field_name").toString)) false else true
      }
    ).map(
      line => {
        "\t`" + line.getAs("field_name").toString + "` " + (
          line.getAs("field_type").toString match {
            case "bigint" | "int8" | "int8range" | "int4range" | "int2vector" => "bigint"
            case "_int2" | "int2" => "smallint"
            case "_int4" | "int4" => "int"
            case "time_stamp" | "timestamp" | "timestamptz" | "datetime" | "datetime2" => "timestamp"
            case "float4" | "float8" => "double"
            case "money" | "numeric" => "decimal(20,4)"
            case "date" => "date"
            case "bool" => "boolean"
            case _ => "string"
          }
          ) + " comment '" + line.getAs("field_comment") + "'"
      }
    ).collect()
      .mkString(",\n")

    val odsDDLMidTwo: String = ",\n\t`etl_is_hard_del` int COMMENT '0,1'," +
      "\n\t`etl_created_ts` timestamp COMMENT 'eim_创建时间'," +
      "\n\t`etl_modified_ts` timestamp COMMENT 'eim_修改时间'" + (
      if ("eim_dt" != partKey) ",\n\t`eim_dt` string" else ""
      )

    val odsDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \n" +
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

    val odsDDLSql = odsDDLPrd + odsDDLMidOne + odsDDLMidTwo + odsDDLSuf
    DDLCreateSqlMap(s"${inputFileName}") = DDLCreateSqlMap(s"${inputFileName}") + "\n" + odsDDLSql

    fileNameAndTableNameMap += inputFileName ->
      mutable.Map("ldg" -> s"ldg.${ddlName}",
        "ldgPk" -> (if ("Y" == ifPhysicalDeletion) s"ldg.${ddlName}_pk" else ""),
        "stg" -> s"stg.${ddlName}",
        "stgPk" -> (if ("Y" == ifPhysicalDeletion) s"stg.${ddlName}_pk" else ""),
        "rej" -> s"rej.ldg_${ddlName}_rej",
        "rejPk" -> (if ("Y" == ifPhysicalDeletion) s"rej.ldg_${ddlName}_pk" else ""),
        "ods" -> s"ods.${ddlName}"
      )


  }

  def getDQDF():DataFrame ={
    this.DQDF
  }

  def getFileNameAndTableNameMap(): mutable.HashMap[String, mutable.Map[String, String]] = {
    this.fileNameAndTableNameMap
  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }


  def getLdgDDLSql(fileName: String): String = {
    DDLCreateSqlMap(s"${fileName}")
  }

  def getLdgDDLSqlList(fileNameList: Array[String]): Array[String] = {
    var arr: Array[String] = Array()
    for (elem <- fileNameList) {
      arr +: getLdgDDLSql(s"${elem}")
    }
    arr
  }

  def writeInpath(fileName: String, jobDate: String): Unit = {
    val writePath: String = s"/home/zhouyahui/scripts/ddl/d${jobDate.replace("-", "")}/${fileName}.sql"
    val file = new File(writePath)
    if (!file.getParentFile.exists) file.getParentFile.mkdirs
    if (!file.exists) file.createNewFile
    val writer = new PrintWriter(file)
    writer.write(getLdgDDLSql(fileName))
    writer.close()

  }

  def writeInpath(fileNameList: Array[String], jobDate: String): Unit = {
    for (elem <- fileNameList) {
      writeInpath(elem, jobDate)
    }
  }
}
