package org.sanofi.loading


import org.apache.spark.sql.DataFrame
import org.sanofi.utils.sparkUtils.getOrCreateSparkSession

import java.text.SimpleDateFormat
import scala.collection.mutable
import java.util.Date


object loadApp extends Serializable {
  def main(args: Array[String]): Unit = {
    //  获取环境变量 url 等和cos路径
    val envArgMap: Map[String, String]
    = Map(
      "BDMP_PG_DATABASES" -> sys.env("BDMP_PG_DATABASES"),
      "BDMP_PG_URL" -> sys.env("BDMP_PG_URL"),
      "BDMP_PG_PORT" -> sys.env("BDMP_PG_PORT"),
      "BDMP_PG_USER" -> sys.env("BDMP_PG_USER"),
      "BDMP_PG_PWD" -> sys.env("BDMP_PG_PWD"),
      "BDMP_SOURCE_FILE_COS_PATH" -> sys.env("BDMP_SOURCE_FILE_COS_PATH")
    )
    val argsMap: mutable.Map[String, String] = argsParse(args)
    argsMap.foreach(println(_))
    if (!argsMap.isEmpty) {
      val spark = getOrCreateSparkSession("local[4]", "test")
//      val spark = getOrCreateSparkSession("yarn", "test")
      val ddl = new createTables(spark, envArgMap, argsMap)
      //      println(ddl.getLdgDDLSql("MD_CL_CONTENT"))
      //      println(ddl.getLdgDDLSqlList(argsMap("tableList").split(",")))
//      ddl.writeInpath(argsMap("tableList").split(","), "2022-09-04")

//      建表语句生成
//val strings: Array[String] = Array("TD_CL_VIDEO_ACTIVITY", "TD_CL_USERANSWERDETAIL", "TD_CL_USERANSWER", "TD_CL_USERACTIVITY", "TD_CL_SHARE_VIDEO_ACTIVITY", "TD_CL_SEARCH_USERACTIVITY_AFFL", "TD_CL_SEARCHACTIVITY", "TD_CL_LIKE_VIDEO_ACTIVITY", "TD_CL_LIKEACTIVITY", "TD_CL_HIPPO_TRANSLATION_ACTIVITY", "TD_CL_HIPPO_REQUESTORIGINALTEXT_ACTIVITY", "TD_CL_HIPPO_PLAYSPEECH_ACTIVITY", "TD_CL_HIPPO_FAVORITE_ACTIVITY", "TD_CL_HIPPO_DOWNLOADPPT_ACTIVITY", "TD_CL_HIPPO_DOWNLOADDOC_ACTIVITY", "TD_CL_HCP_PUSHED", "TD_CL_FAVORITEACTIVITY", "TD_CL_FAQ_VIEW_ACTIVITY", "TD_CL_FAQ_LIKE_ACTIVITY", "TD_CL_DOWNLOADACTIVITY", "TD_CL_CONTENT_PUSHED", "TD_CL_COMMENT_VIDEO_ACTIVITY", "TD_CL_COMMENTSACTIVITY", "TD_CL_CAMPAIGN_ACTIVITY_VIEW_BEHAVIOR", "TD_CL_ARTICLE", "MD_CL_TAG_CUSTOM_DIMENSION", "MD_CL_TAG_CUSTOM_BRAND", "MD_CL_TAG", "MD_CL_QUESTIONGROUPMAP", "MD_CL_QUESTIONGROUP", "MD_CL_QUESTION", "MD_CL_MENU", "MD_CL_MATERIAL_TAG_AFFL", "MD_CL_MATERIAL_SIMILARITY_MAP", "MD_CL_MATERIAL", "MD_CL_MAIN_MATERIAL_REF_MATERIAL_MAP", "MD_CL_FAQ", "MD_CL_CONTENT_VIDEO_MAP", "MD_CL_CONTENT_TAG_AFFL", "MD_CL_CONTENT_PUSHED_TAG_AFFL", "MD_CL_CONTENT_MATERIAL_AFFL", "MD_CL_CONTENT", "MD_CL_CAMPAIGN_ACTIVITY_LISTTEMPLATE_MAP", "MD_CL_CAMPAIGN_ACTIVITY"
//)
//      ddl.writeInpath(strings, "2022-09-04")
      val tablesMap: mutable.HashMap[String, mutable.Map[String, String]] = ddl.getFileNameAndTableNameMap

      val f: DataFrame = ddl.getDQDF
      spark.sql("show databases;").show()

      val qulity = new checkDataQulity(spark,f,tablesMap,argsMap("jobDate"))
      qulity.dq()




//      进行ldg =>stg和rej


      spark.stop()
    }


  }


  def argsParse(args: Array[String]): (mutable.Map[String, String]) = {
    var argsParseFlag: Boolean = true
    val map = new mutable.HashMap[String, String]
    val defaultJobDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

    val standardCmdLine = "参照以下输入方式\n --table tableA,tableB,tableC --jodate yyyy-MM-dd --flag 0,1,0 \n falg可不填，但是有半自动执行的表 需要填写为1 如果为多个 其他的要为0"
    val tablesArgRemind = "缺少 --tables , --table tableA,tableB,tableC"
    val jobDateArgRemind = s"缺少 --jodate , --jodate yyyy-MM-dd     以默认值${defaultJobDate}运行"
    val flagArgRemind = "缺少 --flag , --jodate 0,0,0,1     以默认值全为0运行"

    if (0 == args.length) {
      println(standardCmdLine)
      argsParseFlag = false
    } else if (!args.contains("--tables")) {
      println(tablesArgRemind)
      argsParseFlag = false
    } else if (!args.contains("--jobdate")) {
      println(jobDateArgRemind)
    } else if (!args.contains("--flag")) {
      println(flagArgRemind)
    }

    if (argsParseFlag) {
      args.sliding(2, 2).toList.collect {
        case Array("--tables", tableList: String) => map += ("tableList" -> tableList)
        case Array("--jobdate", jobDate: String) => map += ("jobDate" -> jobDate)
        case Array("--flag", flag: String) => map += ("flag" -> flag)
      }

      if (map.get("flag").isEmpty) map += ("flag" -> ("0" * map("tableList").split(",").length).split("").mkString(","))
      if (map.get("jobDate").isEmpty) map += ("jobDate" -> defaultJobDate)
    }
    map
  }
}
