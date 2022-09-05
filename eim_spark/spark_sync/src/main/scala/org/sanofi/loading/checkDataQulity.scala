package org.sanofi.loading

import org.apache.derby.impl.sql.compile.TableName
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.collection.mutable

class checkDataQulity(spark: SparkSession,
                      df: DataFrame,
                      tablesMap: mutable.HashMap[String, mutable.Map[String, String]],
                      jobDate: String
                     ) extends Serializable {


  //  select * from cl_md_cl_content
  def dq(): Unit = {
    import spark.implicits._
    val keys: Iterable[String] = tablesMap.keys
    //    var pkAndBkMap: mutable.Map[String, String] = mutable.Map("pk" -> "", "bk" -> "")
    val pkAndBkMap: mutable.Map[String, Array[String]] = mutable.Map("pk" -> Array(), "bk" -> Array())
    for (fileName <- keys) {
      val DQRuleMap: Map[String, mutable.Map[String, String]] = df.where(s"file_name = lower('${fileName}')").map(
        r => {
          var key: String = ""
          var checkType: String = ""
          var checkEnum: String = ""
          var enumRange: String = ""
          var checkNull: String = ""


          if (r.getAs("field_alias").equals(null) || 0 == r.getAs("field_alias").toString.length) {
            key = r.getAs("name").toString.toLowerCase()
          } else {
            key = r.getAs("field_alias").toString.toLowerCase()
          }

          checkType = r.getAs("field_type").toString match {
            case "bigint" | "int8" | "int8range" | "int4range" | "int2vector" | "_int2" | "int2" | "_int4" | "int4" => "intCheck"
            case "time_stamp" | "timestamp" | "timestamptz" | "datetime" | "datetime2" => "timestampCheck"
            case "date" => "dateCheck"
            case _ => ""
          }

          checkEnum = r.getAs("if_enum_field").toString match {
            case "Y" => {
              enumRange = r.getAs("value_range")
              "Y"
            }
            case _ => "N"
          }

          checkNull = r.getAs("if_not_null").toString match {
            case "Y" => "Y"
            case _ => "N"
          }

          if ("Y" == r.getAs("primary_key")) pkAndBkMap("pk") :+ key
          if ("Y" == r.getAs("business_key")) pkAndBkMap("bk") :+ key

          key -> mutable.Map("checkType" -> checkType, "checkEnum" -> checkEnum, "enumRange" -> enumRange, "checkNull" -> checkNull)

        }
      ).collect().toMap


      val fieldArr: Array[String] = df.where(s"file_name = lower('${fileName}')")
        .selectExpr("lower(coalesce(if(field_alias='',null,field_alias),name)) as fi_name ")
        .orderBy("index").map(r => r.toString()).collect()

      println(tablesMap(s"${fileName}")("ldg"))
      val selectSql = s"select * from ${tablesMap(s"${fileName}")("ldg")} where eim_dt='${jobDate}'"

      println(DQRuleMap)
      //      不确定能否纯MEMORY      先用 MEMORY+DISK （Map （String->Long）） 未试
      //      spark.sql(selectSql).persist(StorageLevel.MEMORY_ONLY)
      val df1: DataFrame = spark.sql(selectSql)
      df1.persist()

      /*
          * @desc   df1校验列 数值对不对
          * @author   Yav
          * @date 9/5/22 10:04 AM
      */
      import org.apache.spark.sql.catalyst.encoders.RowEncoder
      val schema: StructType = df1.schema
        .add("flag", StringType)
      val columns: Array[String] = df1.columns.filter(_ != "eim_dt")

      df1.filter(x => {
        var target = 0
        for (elem <- columns) {
          if (x.getAs(elem) != elem) target = target + 1
        }
        if (target == 0) false else true
      })
        .map(
          row => {
            var flagArr: Array[String] = Array()
            println(row.schema.fieldNames.mkString(","))
            for (elem <- columns) {
              /*
                  * @desc   一层判断 ，后续判断只校验非空和null值
                  * @author   Yav
                  * @date 9/5/22 10:26 AM
              */
              if (null == row.getAs(elem) || row.getAs(elem).toString.isEmpty) {
                if ("Y" == DQRuleMap(elem)("checkNull")) {
                  flagArr = flagArr :+ s"${elem}列未通过[非空检查]"
                }
              }
              else {
                /*
                    * @desc  一层判断else 校验整形和时间
                    * @author   Yav
                    * @date 9/5/22 10:26 AM
                */
                DQRuleMap(elem)("checkType") match {
                  case "intCheck" =>
                    if (!checkInt(row.getAs(elem).toString)) {
                      flagArr = flagArr :+ s"${elem}列未通过[数据格式校验-数值格式] errValue->${row.getAs(elem).toString}"
                    }
                  case "timestampCheck" =>
                    if (!checkTimestamp(row.getAs(elem).toString, "")) {
                      flagArr = flagArr :+ s"${elem}列未通过[数据格式校验-时间格式] errValue->${row.getAs(elem).toString}"
                    }
                  case "dateCheck" =>
                    if (!checkTimestamp(row.getAs(elem).toString, "")) {
                      flagArr = flagArr :+ s"${elem}列未通过[数据格式校验-时间格式] errValue->${row.getAs(elem).toString}"
                    }
                  case _ => ""
                }

                if ("Y" == DQRuleMap(elem)("checkEnum") &&
                  checkEnum(row.getAs(elem).toString, DQRuleMap(elem)("enumRange"))
                ) {

                }

              }
            }

            val flagStr: String = if (!flagArr.isEmpty) flagArr.mkString("||") else ""

            val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
            buffer.append(flagStr)
            val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
            newRow
          }
        )(RowEncoder(schema)).show()


    }
  }


  def checkInt(str: String): Boolean = {
    var flag: Boolean = true
    for (i <- 0 until str.length) {
      if ("0123456789".indexOf(str.charAt(i)) < 0) flag = false
    }
    flag
  }

  def checkTimestamp(str: String, dateFormat: String): Boolean = {
    var flag: Boolean = true

    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      val date1: Date = format1.parse(str)
      flag = (str.equals(format1.format(date1)))
    }
    catch {
      case e: Exception => {
        flag = false
      }
    }

    if (!flag) {
      val format2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
      try {
        val date2: Date = format2.parse(str)
        flag = (str.equals(format2.format(date2)))
      }
      catch {
        case e: Exception => {
          flag = false
        }
      }
    }

    if (!flag) {
      val format3 = new SimpleDateFormat("yyyy-MM-dd")
      try {
        val date3: Date = format3.parse(str)
        flag = (str.equals(format3.format(date3)))
      }
      catch {
        case e: Exception => {
          flag = false
        }
      }
    }

    flag
  }

  def checkEnum(value: String, range: String): Boolean = {
    val enumRange: Array[String] = range.split(",")
    enumRange.contains(value)
  }
}
