package org.sanofi.loading


import org.sanofi.utils.sparkUtils.getOrCreateSparkSession

import java.text.SimpleDateFormat
import scala.collection.mutable
import java.util.Date


object loadApp {
  def main(args: Array[String]): Unit = {
    //  获取环境变量 url 等和cos路径
     val envArgMap:Map[String, String]
    = Map(
      "bdmpPgDatabases" -> sys.env("BDMP_PG_DATABASES"),
      "bdmpPgUrl" -> sys.env("BDMP_PG_URL"),
      "bdmpPgPort" -> sys.env("BDMP_PG_PORT"),
      "bdmpPgUser" -> sys.env("BDMP_PG_USER"),
      "bdmpPgPwd" -> sys.env("BDMP_PG_PWD"),
      "cosPath" -> sys.env("BDMP_SOURCE_FILE_COS_PATH")
    )

    val argsMap: mutable.Map[String, String] = argsParse(args)

    if (!argsMap.isEmpty) {
      val spark = getOrCreateSparkSession("local[4]", "test")
      val tables = new createTables()
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

      if (map.get("flag") isEmpty) map += ("flag" -> ("0" * map("tableList").split(",").length).split("").mkString(","))
      if (map.get("jobdate") isEmpty) map += ("jobDate" -> defaultJobDate)
    }
    map
  }
}
