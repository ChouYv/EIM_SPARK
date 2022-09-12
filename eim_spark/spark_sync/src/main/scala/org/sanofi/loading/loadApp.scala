package org.sanofi.loading

import org.apache.spark.sql.DataFrame
import org.sanofi.utils.sparkUtils.getOrCreateSparkSession
import org.sanofi.utils.loadUtils._
import org.sanofi.loading.initialTable._
import org.sanofi.loading.dataCheckAndLoad.{dq, dqPk, stgCols}

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
      getDDLSql(fileName, spark, elem._2("dropCreateTableFlag").toBoolean, elem._2("generateTableDDLFlag").toBoolean)


/*
    * @desc onlyCreateTable 默认false  控制是否初次运行
    * @author   Yav
    * @date 9/12/22 11:14 PM
*/
    if(!onlyCreateTable) {

      /*
          * @desc   ldg->stg和rej  进行DQRULE
          * @author   Yav
          * @date 9/11/22 3:18 PM
      */
      dq()
      if ("Y" == ifPhysicalDeletion) {
        dqPk()
      }


      /*
          * @desc   stg->ods
          * @author   Yav
          * @date 9/11/22 3:18 PM
      */
      loadKey
      if ("Y" == ifPhysicalDeletion) {
        val joinCondition: String = pkList.map(x => {
          s"(s.$x = sp.$x)"
        }).mkString(" and  ")
        val concatCols = pkList.map("sp." + _).mkString(",")
        val joinSql = s"select s.*,if(concat($concatCols) is null,0,1) as etl_is_hard_del, date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_created_ts, date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_modified_ts " +
          s"from (select * from $stgTableName where eim_dt='$jobDate') s left join (select * from $stgPkTableName where eim_dt='$jobDate') sp on $joinCondition "
        val toOdsDf: DataFrame = spark.sql(joinSql)
        toOdsDf.createOrReplaceTempView("loadToOds")
      } else {

        spark.sql(s"select *,0 as etl_is_hard_del, date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_created_ts, date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_modified_ts from $stgTableName where eim_dt='$jobDate'")
          .createOrReplaceTempView("loadToOds")
      }


      /*
          * @desc   loadToOds
          * @author   Yav
          * @date 9/12/22 10:59 PM
      */
      val odsColumns: String = spark.sql(s"select * from $odsTableName limit 1").dtypes.map(x => {
        "cast(" + x._1 + " as " + x._2.replace("Type", "") + ") as " + x._1
      }).mkString(",")

      partKey match {
        case "loadKey" => ""
        case "eim_dt" => spark.sql(s"insert overwrite table ${odsTableName} partition(eim_dt) select $odsColumns from loadToOds ")
        case _ => ""
      }
    }





    }


  }
}
