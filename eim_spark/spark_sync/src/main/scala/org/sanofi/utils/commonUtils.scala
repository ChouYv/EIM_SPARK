package org.sanofi.utils

import org.sanofi.loading.dataCheckAndLoad._
object commonUtils extends Serializable {

  def main(args: Array[String]): Unit = {


    println(checkEnum("ddDocument", "Document,MiniSite"))

  }
}
