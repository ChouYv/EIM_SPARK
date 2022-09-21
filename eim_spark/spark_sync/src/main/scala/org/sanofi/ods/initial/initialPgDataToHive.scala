package org.sanofi.ods.initial


import org.apache.spark.internal.Logging
import org.sanofi.utils.sparkUtils._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object initialPgDataToHive extends Serializable with Logging {
  def main(args: Array[String]): Unit = {
    if (args.length > 2) throw new Exception("参数个数不对，至多2个参数，1.执行env 2.文件名ListStr a,b,c")
    /*
        * @desc   执行环境
        * @author   Yav
        * @date 9/20/22 11:11 AM
    */
    val env = args(0)
    /*
        * @desc   文件名，优先取别名，
        * @author   Yav
        * @date 9/20/22 11:11 AM
    */
    var fileListStr = ""
    if (args.length == 2) fileListStr = args(1)

    val fileArr: Array[String] = fileListStr.split(",")

    /*
        * @desc   获取连接参数
        * @author   Yav
        * @date 9/20/22 10:55 AM
    */
    val p = new Properties()
    p.load(this.getClass().getClassLoader.getResourceAsStream("ods-initial.properties"))
    val connMap: mutable.HashMap[String, String] = mutable.HashMap(
      "url" -> p.getProperty(s"$env.initial.pg.url"),
      "user" -> p.getProperty(s"$env.initial.pg.user"),
      "password" -> p.getProperty(s"$env.initial.pg.pwd")
    )


    /*
        * @desc   创建spark会话
        * @author   Yav
        * @date 9/20/22 10:55 AM
    */
    val spark: SparkSession = getOrCreateSparkSession("yarn", "ods_initial_pg_source", "WARN")


    /*
        * @desc   读取pg中 dp_ods_init表数据,形成jobDetailList
        *         过滤掉不需要同步的数据，如果arg(1) 未传数据 则执行所有
        * @author   Yav
        * @date 9/20/22 10:55 AM
    */

    val jobDetailList: DataFrame = spark.read.format("jdbc")
      .options(connMap)
      .option("query", "select * from tech_tmp.dp_ods_init where is_valid ='y'")
      //      .option("dbtable", "tech_tmp.dp_ods_init")
      .load()
      .filter(
        x =>
          if (
            fileArr(0) == "" || fileArr.contains(x.getAs("file_ailas_nm")) || fileArr.contains(x.getAs("file_nm"))
          ) true else false
      )
    /*   
        * @desc   获取执行文件名List
        * @author   Yav
        * @date 9/20/22 4:14 PM 
    */
    import spark.implicits._
    val tuplesJobList: Array[(String, String, String)] = jobDetailList
      .selectExpr("lower(coalesce(if(file_ailas_nm = '' , null, file_ailas_nm),file_nm)) as file_name", "init_sql","source_system_cd")
      .map(
        x =>
          (x.getAs("file_name").toString, x.getAs("init_sql").toString, x.getAs("source_system_cd").toString.toLowerCase)
      )
      .collect()

    for (elem <- tuplesJobList) {

      /*   拼接ods文件名
          * @desc
          * @author   Yav
          * @date 9/20/22 4:31 PM
      */
      val odsFileName: String = "ods." + elem._3 + "_" + elem._1


      /*
          * @desc   获取分区字段
          * @author   Yav
          * @date 9/21/22 10:43 AM
      */
      val fieldList = new ListBuffer[String]
      val parArr: Array[String] = spark.sql(s"show partitions $odsFileName")
        .head(1)(0).get(0).toString.split("/")
      for (elem <- parArr) {
        fieldList.append(elem.split("=")(0))
      }
      val parKey: String = fieldList.mkString(",")

      /*
                * @desc   拼接嵌套SQL 使PG到顺序一致
                * @author   Yav
                * @date 9/20/22 4:45 PM
            */
      //ods 字段顺序
      val cols: Array[String] = spark.sql(s"select * from $odsFileName where 1=0").dtypes.map(x => {
        "cast(" + x._1 + " as " + x._2.replace("Type", "") + ") as " + x._1
      })
      spark.read.format("jdbc")
        .options(connMap)
        .option("query", elem._2.replaceAll(";", ""))
        .load()
        .selectExpr(cols: _*)
        .createOrReplaceTempView("loadToOdsTable")

      /*
          * @desc   执行插入
          * @author   Yav
          * @date 9/21/22 11:25 AM
      */

      spark.sql(s"insert overwrite table $odsFileName  partition($parKey) select * from loadToOdsTable")
    }

    closeSparkSession(spark)




  }
}
