package org.sanofi.loading

import java.util.Properties
import java.io._
import scala.collection.mutable
import scala.sys.process._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.sanofi.utils.loadUtils.jobDate

object initialTable {
  var spark: SparkSession = _
  var cosPath: String = _

  var fieldDf: DataFrame = _

  var ldgTableName: String = _
  var stgTableName: String = _
  var rejTableName: String = _
  var odsTableName: String = _
  var ldgPkTableName: String = _
  var stgPkTableName: String = _
  var rejPkTableName: String = _
  var ifPhysicalDeletion:String= _
  var pkList: mutable.ListBuffer[String] = mutable.ListBuffer()
  var loadKey:String =_
  var partKey: String = _

  def initialPgTable(inputSpark: SparkSession, env: String) = {
    spark = inputSpark
    /*
    * @desc   配置文件获取参数
    * @author   Yav
    * @date 9/11/22 4:46 PM
*/
    val p = new Properties()
    p.load(this.getClass().getClassLoader.getResourceAsStream("eim.properties"))
    //    参数获取位置不对， 后续设计模式需要改造
    cosPath = p.getProperty(s"$env.cos.path")

    val connOptionsMap: mutable.HashMap[String, String] = mutable.HashMap(
      "url" -> s"jdbc:postgresql://${p.getProperty(s"$env.pg.bdmp.url")}:${p.getProperty(s"$env.pg.bdmp.port")}/${p.getProperty(s"$env.pg.bdmp.database")}",
      "user" -> p.getProperty(s"$env.pg.bdmp.user"),
      "password" -> p.getProperty(s"$env.pg.bdmp.pwd")
    )

    /*
        * @desc   PG几张表放入spark操作
        * @author   Yav
        * @date 9/11/22 4:46 PM
    */
    createTempViews(
      connOptionsMap,
      List("app_upt.bdmp_interface", "app_upt.bdmp_interface_field", "app_upt.bdmp_interface_file"),
      List("PgInterface", "PgField", "PgFile")
    )

  }


  def createTempViews(connOptionsMap: mutable.HashMap[String, String], tableNameList: List[String], tmpNameList: List[String]): Unit = {
    for (i <- tableNameList.indices) {
      spark.read.format("jdbc")
        .options(connOptionsMap)
        .option("dbtable", tableNameList(i))
        .load()
        .createOrReplaceTempView(tmpNameList(i))
    }


  }


  def getDDLSql(inputFileName: String, spark: SparkSession, createFlag: Boolean, writeSQL: Boolean) = {
        pkList.clear()
//    val pkList: mutable.ListBuffer[String] = mutable.ListBuffer()
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
        |    bif.file_type,
        |    bif.delimiter,
        |    bif.quote,
        |    lower(bi.source_system)||'_'||lower(coalesce(bif.file_alias,bif.name)) as ddl_name
        |from PgFile bif
        |         left join PgInterface bi on bi.id = bif.interface_id
        |where bi.status=3 and bi.deleted=false and  coalesce(bif.file_alias,bif.name) = '""".stripMargin + inputFileName + "';"
    val df1: DataFrame = spark.sql(sql1)
    df1.count() match {
      case 0L => println(s"传入参数inputFileName为${inputFileName}----->bdmp无此文件相关表信息")
      case 1L => spark.sql(sql1).show()
      case _ => println(s"传入参数inputFileName为${inputFileName}----->对应不止一张表"); df1.show()
    }

    val df1Row: Row = df1.first()

    val pgInterfaceId = df1Row.getAs("bi_id").toString
    val sourceSystem = df1Row.getAs("source_system").toString
    val pgFileId = df1Row.getAs("bif_id").toString
    val loadSolution = df1Row.getAs("load_solution").toString
    val fullDelta = df1Row.getAs("full_delta").toString
     ifPhysicalDeletion = df1Row.getAs("if_physical_deletion").toString
     loadKey = df1Row.getAs("load_key").toString
    val fileType = df1Row.getAs("file_type").toString
    val ddlName = df1Row.getAs("ddl_name").toString

    var fileDelimiter =""
    var fileQuote =""

    var tabCommentCn=""
    if (null!=df1Row.getAs("tab_comment_cn")) {
      tabCommentCn = df1Row.getAs("tab_comment_cn").toString
    }


    if (null == df1Row.getAs("delimiter")) {
      throw new Exception(s"delimiter字段为空")
    } else {
       fileDelimiter = df1Row.getAs("delimiter").toString
    }

    if (null == df1Row.getAs("quote")) {
      throw new Exception(s"quote字段为空")
    } else {
       fileQuote = df1Row.getAs("quote").toString
      if (fileQuote=="\"") fileQuote="\\\""

    }



    val loadKeyList: Array[String] = loadKey.toLowerCase.split(",")

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
        |from PgField where file_id= """.stripMargin + pgFileId + ";"
    val df2: DataFrame = spark.sql(sql2).orderBy("index")
    fieldDf = df2.selectExpr("lower(coalesce(if(field_alias = '' , null, field_alias),name)) as p_name", "*").orderBy("index")
    import spark.implicits._
    /*
      * @desc   获取主键的pkList
      * @author   Yav
      * @date 9/11/22 9:18 PM
  */
    val primaryKeyList: List[String] = df2.select("primary_key")
      .map(_ (0).toString).collect().toList

    if (primaryKeyList.contains("Y")) {

      pkList ++= spark.sql(sql2).selectExpr("lower(coalesce(if(field_alias = '' , null, field_alias),name)) as p_name", "*")
        .orderBy("index")
        .filter(x => "Y" == x.getAs("primary_key"))
        .select("p_name").map(_ (0).toString()).collect().toList
    } else {
      pkList ++= spark.sql(sql2).selectExpr("lower(coalesce(if(field_alias = '' , null, field_alias),name)) as p_name", "*")
        .orderBy("index")
        .filter(x => "Y" == x.getAs("business_key"))
        .select("p_name").map(_ (0).toString()).collect().toList
    }


    val midDF: DataFrame = df2
      .selectExpr(
        "lower(coalesce(if(field_alias='',null,field_alias),name)) as field_name"
        , "lower(field_type) as field_type"
        , "regexp_replace(nvl(fields_comment_en,' ')||'||'||nvl(fields_comment_cn,' '),'[.,`''\\/{}$]',' ') as field_comment")
      .orderBy("index")
    /*
        * @desc   ldg
        * @author   Yav
        * @date 9/11/22 9:21 PM
    */
    val ldgDDLPrd: String = s"drop table if exists ldg.${ddlName};\nCREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName} (\n"
    val ldgDDLMid: String = midDF.map(
      line => "\t`" + line.getAs("field_name").toString + "` string"
    ).collect()
      .mkString(",\n")

    val ldgDDLSuf: String = {
      if ("CSV" == fileType) {
        s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
          s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
          s"with serdeproperties('separatorChar' = '$fileDelimiter','quoteChar'     = '$fileQuote')"+
          s"LOCATION 'cosn://${cosPath}/${sourceSystem}/${ddlName}' \n" +
          s"TBLPROPERTIES ('skip.header.line.count'='1');"
      } else {
        s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string); \n"
      }
    }
    val ldgDDLSql = ldgDDLPrd + ldgDDLMid + ldgDDLSuf
    ldgTableName = s"ldg.$ddlName"

    /*   
        * @desc   ldg-pk
        * @author   Yav
        * @date 9/11/22 9:34 PM
    */
    val ldgPkDDLPrd: String = s"drop table if exists ldg.${ddlName}_pk;\nCREATE EXTERNAL TABLE IF NOT EXISTS ldg.${ddlName}_pk (\n"
    val ldgPkDDLMid: String = pkList.map("\t" + _ + " string").mkString(",\n")
    val ldgPkDDLSuf: String = {
      if ("CSV" == fileType) {
        s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
          s"ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
          s"with serdeproperties('separatorChar' = '$fileDelimiter','quoteChar' = '$fileQuote')"+
        s"LOCATION 'cosn://${cosPath}/${sourceSystem}/${ddlName}_pk' \n" +
          s"TBLPROPERTIES ('skip.header.line.count'='1');"
      } else {
        s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string); \n"
      }
    }
    val ldgPkDDLSql = ldgPkDDLPrd + ldgPkDDLMid + ldgPkDDLSuf
    if ("Y" == ifPhysicalDeletion) println("不应该打印哈")
    ldgPkTableName = s"ldg.${ddlName}_pk"


    /*
        * @desc   stg
        * @author   Yav
        * @date 9/11/22 9:45 PM
    */

    val stgDDLPrd: String = s"drop table if exists stg.${ddlName};\nCREATE TABLE IF NOT EXISTS stg.${ddlName} (\n"
    val stgDDLMid: String = midDF.map(
      line => "\t`" + line.getAs("field_name").toString + "` string comment '" + line.getAs("field_comment").toString.replaceAll("'","") + "'"
    ).collect()
      .mkString(",\n")
    val stgDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val stgDDLSql = stgDDLPrd + stgDDLMid + stgDDLSuf
    stgTableName = s"stg.$ddlName"
    /*
        * @desc    stg-pk
        * @author   Yav
        * @date 9/11/22 9:53 PM
    */

    val stgPkDDLPrd: String = s"drop table if exists stg.${ddlName}_pk;\nCREATE TABLE IF NOT EXISTS stg.${ddlName}_pk (\n"
    val stgPkDDLMid: String = ldgPkDDLMid
    val stgPkDDLSuf: String = stgDDLSuf

    val stgPkDDLSql = stgPkDDLPrd + stgPkDDLMid + stgPkDDLSuf
    stgPkTableName = s"stg.${ddlName}_pk"
    /*
        * @desc   stg_rej
        * @author   Yav
        * @date 9/11/22 9:54 PM
    */
    val rejDDLPrd: String = s"drop table if exists rej.ldg_${ddlName}_rej;\nCREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_rej (\n"
    val rejDDLMid: String = stgDDLMid + ",\n\t`flag` string comment '标签'"
    val rejDDLSuf: String = s") \nCOMMENT '${tabCommentCn}' \nPARTITIONED BY (`eim_dt` string) \n" +
      s"ROW FORMAT DELIMITED \n" +
      s"FIELDS TERMINATED BY '\\001' \n" +
      s"LINES TERMINATED BY '\\n' \n" +
      s"stored as orc;"

    val rejDDLSql = rejDDLPrd + rejDDLMid + rejDDLSuf
    rejTableName = s"rej.ldg_${ddlName}_rej"
    /*
        * @desc   rej_pk
        * @author   Yav
        * @date 9/11/22 9:55 PM
    */
    val rejPkDDLPrd: String = s"drop table if exists rej.ldg_${ddlName}_pk;\nCREATE TABLE IF NOT EXISTS rej.ldg_${ddlName}_pk (\n"
    val rejPkDDLMid: String = ldgPkDDLMid + ",\n\t`flag` string "
    val rejPkDDLSuf: String = stgDDLSuf

    val rejPkDDLSql = rejPkDDLPrd + rejPkDDLMid + rejPkDDLSuf
    rejPkTableName=s"rej.ldg_${ddlName}_pk"

    /*
        * @desc   ods
        * @author   Yav
        * @date 9/11/22 9:57 PM
    */


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
        if (partKey == "loadKey" && loadKeyList.contains(i.getAs("field_name").toString)) false else true
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
          ) + " comment '" + line.getAs("field_comment").toString.replaceAll("'","") + "'"
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
    odsTableName = s"ods.$ddlName"

    /*
        * @desc   createFlag:Boolean,writeSQL:Boolean  判断是否执行  是否落盘
        * @author   Yav
        * @date 9/11/22 10:11 PM
    */

    val executionSql: String = {
      if ("Y" == ifPhysicalDeletion) {
        ldgDDLSql + "\n" + stgDDLSql + "\n" +
          rejDDLSql + "\n" + odsDDLSql + "\n" +
          ldgPkDDLSql + "\n" + stgPkDDLSql + "\n" +
          rejPkDDLSql
      } else {
        ldgDDLSql + "\n" + stgDDLSql + "\n" +
          rejDDLSql + "\n" + odsDDLSql
      }
    }

    if (createFlag) doHiveSql(executionSql)
    if (writeSQL) writeInpath(inputFileName, executionSql)

  }


  def doHiveSql(executionSql: String) = {
    val sql: Seq[String] = Seq("hive", "-e", executionSql)
    val builder: ProcessBuilder = Process(sql)
    val exitCode: Int = builder.!
    //    println(exitCode)
  }

  def writeInpath(fileName: String, downSql: String): Unit = {
    val user: String = System.getProperty("user.name")
    val writePath: String = s"/home/$user/scripts/ddl/d${jobDate.replace("-", "")}/${fileName}.sql"
    val file = new File(writePath)
    if (!file.getParentFile.exists) file.getParentFile.mkdirs
    if (!file.exists) file.createNewFile
    val writer = new PrintWriter(file)
    writer.write(downSql)
    writer.close()

  }


  def getInitialSparkSession(): SparkSession = {
    this.spark
  }


}

