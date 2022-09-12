package org.sanofi.utils
import scala.sys.process._

import scala.collection.mutable
import java.util.Properties
object commonUtils extends Serializable {

  def main(args: Array[String]): Unit = {
//    val cmd: String = "hive -e 'select 1;'"
//    val cmd: String = "hive -e  \"select 1;\""
//    println(cmd)
//    val result: String = cmd.!!
//    println(result)

    val sql: Seq[String] = Seq("hive", "-e", "select 1")
    val builder: ProcessBuilder = Process(sql)
    val exitCode: Int = builder.!
    println(exitCode)
  }
}
