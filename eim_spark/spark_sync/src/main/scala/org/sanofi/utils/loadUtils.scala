package org.sanofi.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.InputStream
import java.util.Properties
import scala.collection.mutable
import java.text.SimpleDateFormat
import java.util.Date
import org.sanofi.model.Constants.cmdMessage._
import org.sanofi.utils.sparkUtils.getOrCreateSparkSession

import scala.collection.mutable.ListBuffer


object loadUtils {
  def parseArgs(args: Array[String]): mutable.HashMap[String, String] = {

    var envParameter = ""
    var modelParameter = ""
    val parameterMap = new mutable.HashMap[String, String]
    val defaultJobDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

    if (0 == args.length) throw new Exception(STANDARD_CMD_LINE)
    if (!args.contains("--model")) throw new Exception(MISS_RUNNING_MODEL)
    if (!args.contains("--env")) throw new Exception(MISS_RUNNING_ENVIRONMENT)

    args.sliding(2, 2).toList.collect {
      case Array("--env", value: String) => envParameter = value
      case Array("--model", value: String) => modelParameter = value
    }

    val spark = getOrCreateSparkSession("local[4]", s"auto_load_$defaultJobDate", "WARN")

    setEnvToMap(envParameter, parameterMap)

    modelParameter match {
      case "auto" => getAutoParameter(parameterMap, spark)
      case "manual" => getManualParameter(parameterMap, args)
    }

    removeOtherKey(parameterMap)

    if (parameterMap.get("jobDate").isEmpty) parameterMap += ("jobDate" -> defaultJobDate)

    parameterMap

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
    if (null != p.getProperty(s"$env.cos.path")) map += ("path" -> p.getProperty(s"$env.cos.path")) else throw new NoSuchElementException(s"未获取 $env.cos.path 值，检查配置文件")
  }


  /*
      * @desc   通过配置的源表获取 同步文件list等信息
      * @author   Yav
      * @date 9/8/22 4:48 AM
  */
  def getAutoParameter(map: mutable.HashMap[String, String], spark: SparkSession): Unit = {
    var fileList = new ListBuffer[String]
    var flagList = new ListBuffer[String]
    val tuples: Array[(String, String)] = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://" + map("url") + ":" + map("port") + "/" + map("database"))
      .option("user", map("user"))
      .option("password", map("pwd"))
      .option("dbtable", "app_upt.load_file_list")
      .load()
      .where("on_line=true")
      .select("load_file_name", "drop_table_flag")
      .collect()
      .map(x => x.getAs("load_file_name").toString -> x.getAs("drop_table_flag").toString)

    tuples.foreach(x => {
      fileList = fileList :+ x._1
      flagList = flagList :+ x._2
    })
    map.remove("url")
    map.remove("user")
    map.remove("pwd")
    map.remove("dbtable")
    map.remove("port")
    map.remove("database")
    map.put("fileList", fileList.mkString(","))
    map.put("flagList", flagList.mkString(","))
  }

  /*
      * @desc   通过传入的args获取
      * @author   Yav
      * @date 9/8/22 4:49 AM
  */
  def getManualParameter(map: mutable.HashMap[String, String], args: Array[String]): Unit = {

    args.sliding(2, 2).toList.collect {
      case Array("--file", tableList: String) => map += ("fileList" -> tableList)
      case Array("--jobdate", jobDate: String) => map += ("jobDate" -> jobDate)
      case Array("--flag", flag: String) => map += ("flagList" -> flag)
    }
    if (map.get("flagList").isEmpty) map += ("flagList" -> ("0" * map("fileList").split(",").length).split("").map(_.replace("0","true")).mkString(","))
  }

  def main(args: Array[String]): Unit = {
    val stringToString = parseArgs(args)
    println(stringToString)
  }


  def getPostgresDf(sparkSession: SparkSession, connOptionsMap: Map[String, String], tableName: String): DataFrame = {
    sparkSession.read.format("jdbc")
      .options(connOptionsMap)
      .option("dbtable", tableName)
      .load()
  }

  def removeOtherKey(map: mutable.HashMap[String, String]):Unit = {
    map.remove("url")
    map.remove("user")
    map.remove("pwd")
    map.remove("dbtable")
    map.remove("port")
    map.remove("database")
  }
}
