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


    if (0 == args.length) throw new Exception(STANDARD_CMD_LINE)
    if (!args.contains("--model")) throw new Exception(MISS_RUNNING_MODEL)
    if (!args.contains("--env")) throw new Exception(MISS_RUNNING_ENVIRONMENT)


    args.sliding(2, 2).toList.collect {
      case Array("--env", value: String) => envParameter = value
      case Array("--model", value: String) => modelParameter = value
    }

    setEnvToMap(envParameter, parameterMap)

    val spark = getOrCreateSparkSession("local[4]", "test", "WARN")

    getAutoParameter(parameterMap,spark)


    parameterMap


    //    val defaultJobDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    //
    //    args.sliding(2, 2).toList.collect {
    //      case Array("--model", tableList: String) => map += ("model" -> tableList)
    //      case Array("--env", tableList: String) => map += ("env" -> tableList)
    //      case Array("--file", tableList: String) => map += ("fileList" -> tableList)
    //      case Array("--jobdate", jobDate: String) => map += ("jobDate" -> jobDate)
    //      case Array("--flag", flag: String) => map += ("flag" -> flag)
    //    }
    //
    //
    //    if (!args.contains("--tables")) throw new Exception(MISS_RUNNING_FILE_LIST)
    //    if (!args.contains("--jobdate")) println(MISS_RUNNING_JOB_DATE)
    //    if (!args.contains("--flag")) println(MISS_RUNNING_FLAG)
    //
    //
    //    if (map.get("flag").isEmpty) map += ("flag" -> ("0" * map("tableList").split(",").length).split("").mkString(","))
    //    if (map.get("jobDate").isEmpty) map += ("jobDate" -> defaultJobDate)
    //
    //    map
  }
  /*
      * @desc     切换不同环境的源数据库连接信息
      * @author    zhouyahui
      * @date    2022/9/8 14:04
  */

  def setEnvToMap(env: String, map: mutable.HashMap[String, String]): Unit = {
    val p = new Properties()
    p.load(this.getClass().getClassLoader.getResourceAsStream("eim.properties"))

    if (null != p.getProperty(s"${env}.source.url")) map += ("url" -> p.getProperty(s"${env}.source.url")) else throw new NoSuchElementException(s"未获取 ${env}.source.url 值，检查配置文件")
    if (null != p.getProperty(s"${env}.source.port")) map += ("port" -> p.getProperty(s"${env}.source.port")) else throw new NoSuchElementException(s"未获取 ${env}.source.port 值，检查配置文件")
    if (null != p.getProperty(s"${env}.source.user")) map += ("user" -> p.getProperty(s"${env}.source.user")) else throw new NoSuchElementException(s"未获取 ${env}.source.user 值，检查配置文件")
    if (null != p.getProperty(s"${env}.source.pwd")) map += ("pwd" -> p.getProperty(s"${env}.source.pwd")) else throw new NoSuchElementException(s"未获取 ${env}.source.pwd 值，检查配置文件")
    if (null != p.getProperty(s"${env}.source.database")) map += ("database" -> p.getProperty(s"${env}.source.database")) else throw new NoSuchElementException(s"未获取 ${env}.source.database 值，检查配置文件")
  }


  def getAutoParameter(map: mutable.HashMap[String, String], spark: SparkSession): Unit = {
    val fileList = new ListBuffer[String]
    val flagList = new ListBuffer[String]
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

    tuples.foreach(x=>{
      fileList:+x._1
      flagList:+x._2
    })

    map.remove("url")
    map.remove("user")
    map.remove("password")
    map.remove("dbtable")
    map.remove("port")
    map.remove("database")
    map.put("fileList",fileList.mkString(","))
    map.put("flagList",flagList.mkString(","))

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


}
