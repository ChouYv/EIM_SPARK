package org.sanofi.model

object Constants {
  object cmdMessage extends Enumeration {
    type cmdMessage = Value
    val STANDARD_CMD_LINE = "输入异常----------参照以下输入方式\n --model auto/manual --env dev --table tableA,tableB,tableC --jobdate yyyy-MM-dd --flag true,false,true"
    val MISS_RUNNING_MODEL = "运行模式异常----------参照以下输入方式\n --model (auto or manual)"
    val MISS_RUNNING_ENVIRONMENT = "运行环境异常----------参照以下输入方式\n --env dev"
    val MISS_RUNNING_FILE_LIST = "执行文件列表异常----------参照以下输入方式\n --table tableA,tableB,tableC"
    val MISS_RUNNING_JOB_DATE = "未获取指定执行时间 以当前时间执行----------参照以下输入方式\n --jobdate yyyy-MM-dd"
    val MISS_RUNNING_FLAG = "缺少是否删表建表标识 ,以默认值全为false运行----------参照以下输入方式\n --flag true,false,true,false"
  }
}
