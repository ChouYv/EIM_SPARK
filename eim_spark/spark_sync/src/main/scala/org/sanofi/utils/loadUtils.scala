package org.sanofi.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

import org.sanofi.model.Constants.cmdMessage._
import org.sanofi.utils.sparkUtils._

import java.time.LocalDate
import scala.collection.mutable


object loadUtils {
  var jobDate = ""
  var envParameter = ""
  val jobMap: mutable.HashMap[String, mutable.HashMap[String, String]] = mutable.HashMap()
//  快捷建表- 初次运行
  var onlyCreateTable:Boolean = false
  def parseArgs(args: Array[String]): mutable.HashMap[String, mutable.HashMap[String, String]] = {

    var modelParameter = ""
    val parameterMap = new mutable.HashMap[String, String]

    if (0 == args.length) throw new Exception(STANDARD_CMD_LINE)
    if (!args.contains("--model")) throw new Exception(MISS_RUNNING_MODEL)
    if (!args.contains("--env")) throw new Exception(MISS_RUNNING_ENVIRONMENT)

    args.sliding(2, 2).toList.collect {
      case Array("--env", value: String) => envParameter = value
      case Array("--model", value: String) => modelParameter = value
      case Array("--jobdate", value: String) => jobDate = value
      case Array("--first", value: String) => onlyCreateTable = value.toBoolean
    }

    if ("" == jobDate) jobDate = getDateyyyyMMdd

    val spark = getOrCreateSparkSession("local[2]", s"initial_auto_load_$jobDate", "WARN")

    setEnvToMap(envParameter, parameterMap)

    modelParameter match {
      case "auto" => getAutoParameter(parameterMap, spark, jobMap)
      case "manual" => getManualParameter(parameterMap, args, jobMap)
    }

    closeSparkSession(spark)
    jobMap

  }

  def getDateyyyyMMdd(): String = {
    LocalDate.now().minusDays(1).toString
  }

  /*
      * @desc     切换不同环境的源数据库连接信息
      * @author    zhouyahui
      * @date    2022/9/8 14:04
  */

  def setEnvToMap(env: String, map: mutable.HashMap[String, String]): Unit = {
    val p = new Properties()
    p.load(this.getClass().getClassLoader.getResourceAsStream("eim.properties"))

    if (null != p.getProperty(s"$env.source.url")) map += ("url" -> p.getProperty(s"$env.source.url")) else throw new NoSuchElementException(s"未获取 $env.source.url 值，检查配置文件")
    if (null != p.getProperty(s"$env.source.port")) map += ("port" -> p.getProperty(s"$env.source.port")) else throw new NoSuchElementException(s"未获取 $env.source.port 值，检查配置文件")
    if (null != p.getProperty(s"$env.source.user")) map += ("user" -> p.getProperty(s"$env.source.user")) else throw new NoSuchElementException(s"未获取 $env.source.user 值，检查配置文件")
    if (null != p.getProperty(s"$env.source.pwd")) map += ("pwd" -> p.getProperty(s"$env.source.pwd")) else throw new NoSuchElementException(s"未获取 $env.source.pwd 值，检查配置文件")
    if (null != p.getProperty(s"$env.source.database")) map += ("database" -> p.getProperty(s"$env.source.database")) else throw new NoSuchElementException(s"未获取 $env.source.database 值，检查配置文件")

  }


  /*
      * @desc   通过配置的源表获取 同步文件list等信息
      * @author   Yav
      * @date 9/8/22 4:48 AM
  */
  def getAutoParameter(map: mutable.HashMap[String, String], spark: SparkSession, jobMap: mutable.HashMap[String, mutable.HashMap[String, String]]): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://" + map("url") + ":" + map("port") + "/" + map("database"))
      .option("user", map("user"))
      .option("password", map("pwd"))
      .option("dbtable", "app_upt.load_file_list")
      .load()
      .where("on_line=true")
      .select("load_file_name", "drop_create_table_flag", "generate_tableDDL_flag")
      .collect()
      .map(x =>
        (x.getAs("load_file_name").toString ->
          mutable.HashMap("dropCreateTableFlag" -> x.getAs("drop_create_table_flag").toString,
            "generateTableDDLFlag" -> x.getAs("generate_tableDDL_flag").toString))
      ).foreach(
      (line: (String, mutable.HashMap[String, String])) => {
        val k = line._1
        val v = line._2
        jobMap += (k -> v)
      }
    )


  }

  /*
      * @desc   通过传入的args获取
      * @author   Yav
      * @date 9/8/22 4:49 AM
  */
  def getManualParameter(inputMap: mutable.HashMap[String, String], args: Array[String], jobMap: mutable.HashMap[String, mutable.HashMap[String, String]]): Unit = {

    args.sliding(2, 2).toList.collect {
      case Array("--file", tableList: String) => inputMap += ("fileList" -> tableList)
      case Array("--jobdate", jobDate: String) => inputMap += ("jobDate" -> jobDate)
      case Array("--dropflag", flag: String) => inputMap += ("dropFlag" -> flag)
      case Array("--generateflag", flag: String) => inputMap += ("generateFlag" -> flag)
    }
    if (inputMap.get("dropFlag").isEmpty) inputMap += ("dropFlag" -> ("0" * inputMap("fileList").split(",").length).split("").map(_.replace("0", "true")).mkString(","))
    if (inputMap.get("generateFlag").isEmpty) inputMap += ("generateFlag" -> ("0" * inputMap("fileList").split(",").length).split("").map(_.replace("0", "true")).mkString(","))

    for (i <- 0 until inputMap("fileList").split(",").length) {
      jobMap +=
        inputMap("fileList").split(",")(i) ->
          mutable.HashMap(
            "dropCreateTableFlag" -> inputMap("dropFlag").split(",")(i),
            "generateTableDDLFlag" -> inputMap("generateFlag").split(",")(i)
          )
    }

  }

//  def main(args: Array[String]): Unit = {
//    val stringToString = parseArgs(args)
//    println(stringToString)
//  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }

}
