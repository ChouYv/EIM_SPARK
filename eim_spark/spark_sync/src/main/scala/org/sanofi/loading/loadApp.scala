package org.sanofi.loading

import org.sanofi.utils.sparkUtils.getOrCreateSparkSession

import org.sanofi.utils.loadUtils._
import org.sanofi.loading.initialTable._

object loadApp {
  def main(args: Array[String]): Unit = {
    /*
        * @desc   获取jobMap 格式如下
        *    HashMap (
        *             MD_CL_CONTENT -> Map(dropCreateTableFlag -> true, generateTableDDLFlag -> true)
        *             MD_CL_CONTENT_VIDEO_MAP -> Map(dropCreateTableFlag -> false, generateTableDDLFlag -> true)
        *                                       )
        *
        *       jobDate  yyyy-MM-dd   2022-09-11
        * @author   Yav
        * @date 9/11/22 3:11 PM
    */
    parseArgs(args)
    //    println(jobDate + "\n")
    println(jobMap + "\n")
    //    println(envParameter + "\n")
    val spark = getOrCreateSparkSession("local[4]", s"auto_load_$jobDate", "WARN")


    initialPgTable(spark, envParameter)


    for (elem <- jobMap) {
      val fileName = elem._1
      /*
          * @desc   是否删表建表,是否落盘
          * @author   Yav
          * @date 9/11/22 3:17 PM
      */
      if (elem._2("dropCreateTableFlag").toBoolean || elem._2("generateTableDDLFlag").toBoolean) {
        getDDLSql(fileName, spark, elem._2("dropCreateTableFlag").toBoolean, elem._2("generateTableDDLFlag").toBoolean)
      }

      /*
          * @desc   ldg->stg和rej  进行DQRULE
          * @author   Yav
          * @date 9/11/22 3:18 PM
      */


      /*
          * @desc   stg->ods
          * @author   Yav
          * @date 9/11/22 3:18 PM
      */
    }


  }
}
