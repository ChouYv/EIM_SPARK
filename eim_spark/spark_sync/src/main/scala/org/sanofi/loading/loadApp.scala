package org.sanofi.loading
import org.sanofi.utils.loadUtils._

import scala.collection.mutable

object loadApp {
  def main(args: Array[String]): Unit = {
    val parameterMap: mutable.HashMap[String, String] = parseArgs(args)
    println(parameterMap)
  }
}
