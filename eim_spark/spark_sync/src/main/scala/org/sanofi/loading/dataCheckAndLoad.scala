package org.sanofi.loading

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.sanofi.loading.initialTable._
import org.apache.spark.sql.types.{StringType, StructType}
import scala.collection.mutable
import org.sanofi.utils.loadUtils.jobDate

import java.text.SimpleDateFormat
import java.util.Date

object dataCheckAndLoad extends Serializable {
  val spark: SparkSession = getInitialSparkSession
  var stgCols:String = _

  def dq() = {
    println(ldgTableName)
    import spark.implicits._
    val DQRuleMap: Map[String, mutable.HashMap[String, String]] = fieldDf.map(
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


        key -> mutable.HashMap("checkType" -> checkType, "checkEnum" -> checkEnum, "enumRange" -> enumRange, "checkNull" -> checkNull)

      }
    ).collect().toMap
    DQRuleMap.foreach(println(_))

    val jobPkList: List[String] = pkList.toList

    val selectSql = s"select * from $ldgTableName where eim_dt='${jobDate}'"

    val df1: DataFrame = spark.sql(selectSql)
    df1.persist()

    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    val schema: StructType = df1.schema
      .add("flag", StringType)
    val columns: Array[String] = df1.columns.filter(_ != "eim_dt")
    println(columns.mkString(","))
    /*
        * @desc
        * @author   Yav
        * @date 9/6/22 5:29 AM
    */
    val duplicateFieldList: List[String] = df1.filter(x => {
      var target = 0
      for (elem <- columns) {
        if (x.getAs(elem) != elem) target = target + 1
      }
      if (target == 0) false else true
    }).map(x => {
      var key = ""
      val value = 1
      if (0 == jobPkList.length) {
        (x.getAs(jobPkList(0)).toString, value)
      } else {
        for (i <- 0 until (jobPkList.length)) {
          key = key + x.get(i)
        }
        (key, value)
      }
    }).toDF("key", "value").groupBy("key").count().where("count > 1").map(x => x.get(0).toString).collect().toList


    val checkDF: Dataset[Row] = df1.filter(x => {
      var target = 0
      for (elem <- columns) {
        if (x.getAs(elem) != elem) target = target + 1
      }
      if (target == 0) false else true
    })
      .map(
        row => {
          val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
          var flagArr: Array[String] = Array()
          var errLine :String=""
          var initInt = 0
          for (elem <- columns) {

            if (null == row.getAs(elem)){
              if (errLine=="")errLine="????????????"
            }
            /*
                * @desc   ???????????? ?????????????????????????????????null???
                * @author   Yav
                * @date 9/5/22 10:26 AM
            */
            if (null == row.getAs(elem) || row.getAs(elem).toString.isEmpty) {
              if ("Y" == DQRuleMap(elem)("checkNull") && row.getAs(elem)!=null) {
                flagArr = flagArr :+ s"${elem}????????????[????????????]"
              }
            }
            else {
              /*
                  * @desc  ????????????else ?????????????????????
                  * @author   Yav
                  * @date 9/5/22 10:26 AM
              */
              DQRuleMap(elem)("checkType") match {
                case "intCheck" =>
                  if (!checkInt(row.getAs(elem).toString)) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  }
                case "timestampCheck" =>
                  if (!checkTimestamp(row.getAs(elem).toString, "")) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  } else {
                    buffer.update(initInt,updateTimestamp(row.getAs(elem).toString))
                }
                case "dateCheck" =>
                  if (!checkTimestamp(row.getAs(elem).toString, "")) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  }
                case _ => ""
              }
              /*
                  * @desc   ???????????????   null??? ???????????????
                  * @author   Yav
                  * @date 9/5/22 10:00 PM
              */
              if ("Y" == DQRuleMap(elem)("checkEnum") &&
                !checkEnum(row.getAs(elem).toString, DQRuleMap(elem)("enumRange"))
              ) {
                flagArr = flagArr :+ s"${elem}????????????[???????????????] errValue->${row.getAs(elem).toString},range->${DQRuleMap(elem)("enumRange")}"
              }

            }
            initInt = initInt+1
          }
          var key = ""
          if (0 == jobPkList.length) {
            key = row.getAs(jobPkList(0)).toString
          } else {
            for (i <- 0 until (jobPkList.length)) {
              key = key + row.get(i)
            }
          }
          if (duplicateFieldList.contains(key)) flagArr = flagArr :+ s"???????????????[???????????????]"
          if(errLine!="") flagArr = flagArr:+errLine
          val flagStr: String = if (!flagArr.isEmpty) flagArr.mkString("||") else ""



          /*
              * @desc   ???????????????????????? yyyy-MM-dd HH:mm:ss
              * @author   Yav
              * @date 9/12/22 6:51 PM
          */


          buffer.append(flagStr)
          val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
          newRow
        }
      )(RowEncoder(schema))


    /*
        * @desc   ???????????? ldg->stg???rej
        * @author   Yav
        * @date 9/6/22 9:10 AM
    */
    val stgAndRejArr: Array[String] = checkDF.columns.filter(!Array("eim_dt", "flag").contains(_))
    checkDF.createOrReplaceTempView("ldgToStgTable")
    stgCols=stgAndRejArr.mkString(",")

    val sumLong: Long = checkDF.count()
    println(s"${ldgTableName}???????????????:" +sumLong+"???")

    val insertStgSql = s"insert overwrite table ${stgTableName} partition(eim_dt='${jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")} from ldgToStgTable where flag ='' "
    spark.sql(insertStgSql)
    val stgLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")} from ldgToStgTable where flag ='' ").count()
    println(s"?????????${stgTableName}???????????????:" +stgLong+"???")

    val insertRejSql = s"insert overwrite table ${rejTableName} partition(eim_dt='${jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")},flag from ldgToStgTable where (${stgAndRejArr.map(_+" is not null ").mkString(" or ")}) and flag <>'' "
    spark.sql(insertRejSql)
    val rejLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},flag from ldgToStgTable where (${stgAndRejArr.map(_ + " is not null ").mkString(" or ")}) and flag <>'' ").count()
    println(s"?????????${rejTableName}???????????????:" +rejLong+"???")

    val nullLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},flag from ldgToStgTable where (${stgAndRejArr.map(_ + " is  null ").mkString(" and ")}) and flag <>''").count()
    println(s"${ldgTableName}?????????????????????????????????:" +nullLong+"???")





  }

  def dqPk()={
    import spark.implicits._
    val DQRuleMap: Map[String, mutable.HashMap[String, String]] = fieldDf.filter(x=>pkList.toList.contains(x.getAs("p_name"))).map(
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


        key -> mutable.HashMap("checkType" -> checkType, "checkEnum" -> checkEnum, "enumRange" -> enumRange, "checkNull" -> checkNull)

      }
    ).collect().toMap
    DQRuleMap.foreach(println(_))

    val jobPkList: List[String] = pkList.toList
//
    val selectSql = s"select * from $ldgPkTableName where eim_dt='${jobDate}'"

    val df1: DataFrame = spark.sql(selectSql)
    df1.persist()

    import org.apache.spark.sql.catalyst.encoders.RowEncoder
    val schema: StructType = df1.schema
      .add("flag", StringType)
    val columns: Array[String] = df1.columns.filter(_ != "eim_dt")
    println(columns.mkString(","))
    /*
        * @desc
        * @author   Yav
        * @date 9/6/22 5:29 AM
    */
    val duplicateFieldList: List[String] = df1.filter(x => {
      var target = 0
      for (elem <- columns) {
        if (x.getAs(elem) != elem) target = target + 1
      }
      if (target == 0) false else true
    }).map(x => {
      var key = ""
      val value = 1
      if (0 == jobPkList.length) {
        (x.getAs(jobPkList(0)).toString, value)
      } else {
        for (i <- 0 until (jobPkList.length)) {
          key = key + x.get(i)
        }
        (key, value)
      }
    }).toDF("key", "value").groupBy("key").count().where("count > 1").map(x => x.get(0).toString).collect().toList


    val checkPkDF: Dataset[Row] = df1.filter(x => {
      var target = 0
      for (elem <- columns) {
        if (x.getAs(elem) != elem) target = target + 1
      }
      if (target == 0) false else true
    })
      .map(
        row => {
          val buffer = Row.unapplySeq(row).get.map(_.asInstanceOf[String]).toBuffer
          var flagArr: Array[String] = Array()
          var errLine: String = ""
          var initInt = 0
          for (elem <- columns) {

            if (null == row.getAs(elem)) {
              if (errLine == "") errLine = "????????????"
            }
            /*
                * @desc   ???????????? ?????????????????????????????????null???
                * @author   Yav
                * @date 9/5/22 10:26 AM
            */
            if (null == row.getAs(elem) || row.getAs(elem).toString.isEmpty) {
              if ("Y" == DQRuleMap(elem)("checkNull") && row.getAs(elem) != null) {
                flagArr = flagArr :+ s"${elem}????????????[????????????]"
              }
            }
            else {
              /*
                  * @desc  ????????????else ?????????????????????
                  * @author   Yav
                  * @date 9/5/22 10:26 AM
              */
              DQRuleMap(elem)("checkType") match {
                case "intCheck" =>
                  if (!checkInt(row.getAs(elem).toString)) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  }
                case "timestampCheck" =>
                  if (!checkTimestamp(row.getAs(elem).toString, "")) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  } else {
                    buffer.update(initInt, updateTimestamp(row.getAs(elem).toString))
                  }
                case "dateCheck" =>
                  if (!checkTimestamp(row.getAs(elem).toString, "")) {
                    flagArr = flagArr :+ s"${elem}????????????[??????????????????-????????????] errValue->${row.getAs(elem).toString}"
                  }
                case _ => ""
              }
              /*
                  * @desc   ???????????????   null??? ???????????????
                  * @author   Yav
                  * @date 9/5/22 10:00 PM
              */
              if ("Y" == DQRuleMap(elem)("checkEnum") &&
                !checkEnum(row.getAs(elem).toString, DQRuleMap(elem)("enumRange"))
              ) {
                flagArr = flagArr :+ s"${elem}????????????[???????????????] errValue->${row.getAs(elem).toString},range->${DQRuleMap(elem)("enumRange")}"
              }

            }
            initInt = initInt + 1
          }
          var key = ""
          if (0 == jobPkList.length) {
            key = row.getAs(jobPkList(0)).toString
          } else {
            for (i <- 0 until (jobPkList.length)) {
              key = key + row.get(i)
            }
          }
          if (duplicateFieldList.contains(key)) flagArr = flagArr :+ s"???????????????[???????????????]"
          if (errLine != "") flagArr = flagArr :+ errLine
          val flagStr: String = if (!flagArr.isEmpty) flagArr.mkString("||") else ""



          /*
              * @desc   ???????????????????????? yyyy-MM-dd HH:mm:ss
              * @author   Yav
              * @date 9/12/22 6:51 PM
          */


          buffer.append(flagStr)
          val newRow: Row = new GenericRowWithSchema(buffer.toArray, schema)
          newRow
        }
      )(RowEncoder(schema))


    /*
        * @desc   ???????????? ldg->stg???rej
        * @author   Yav
        * @date 9/6/22 9:10 AM
    */
    val stgAndRejArr: Array[String] = checkPkDF.columns.filter(!Array("eim_dt", "flag").contains(_))
    checkPkDF.show()
    checkPkDF.createOrReplaceTempView("ldgPkToStgPkTable")


    val sumLong: Long = checkPkDF.count()
    println(s"${ldgPkTableName}???????????????:" + sumLong + "???")

    val insertStgPkSql = s"insert overwrite table ${stgPkTableName} partition(eim_dt='${jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")} from ldgPkToStgPkTable where flag ='' "
    spark.sql(insertStgPkSql)
    val stgLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")} from ldgPkToStgPkTable where flag ='' ").count()
    println(s"?????????${stgPkTableName}???????????????:" + stgLong + "???")

    val insertPkRejSql = s"insert overwrite table ${rejPkTableName} partition(eim_dt='${jobDate}') " +
      s"select ${stgAndRejArr.mkString(",")},flag from ldgPkToStgPkTable where (${stgAndRejArr.map(_ + " is not null ").mkString(" or ")}) and flag <>'' "
    spark.sql(insertPkRejSql)
    val rejLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},flag from ldgPkToStgPkTable where (${stgAndRejArr.map(_ + " is not null ").mkString(" or ")}) and flag <>'' ").count()
    println(s"?????????${rejPkTableName}???????????????:" + rejLong + "???")

    val nullLong: Long = spark.sql(s"select ${stgAndRejArr.mkString(",")},flag from ldgPkToStgPkTable where (${stgAndRejArr.map(_ + " is  null ").mkString(" and ")}) and flag <>''").count()
    println(s"${ldgPkTableName}?????????????????????????????????:" + nullLong + "???")
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
      val format2 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
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


  def updateTimestamp(str: String): String = {
    var newStr = str
    if(str.contains("/")){
      val format2 = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
      val format3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val date2: Date = format2.parse(str)
      newStr = format3.format(date2)
    }

    if (str.length=="yyyy-MM-dd".length)  newStr=str+" 00:00:00"
    newStr
  }








  def checkEnum(value: String, range: String): Boolean = {

    val enumRange: Array[String] = range.split(",")
    enumRange.contains(value)
  }

}
