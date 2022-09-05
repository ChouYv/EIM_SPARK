package org.sanofi.loading

case class fileAndTableName (
                   fileName :String,
                   ldgTableName :String,
                   ldgPkTableName:Option[String],
                   stgTableName:String,
                   stgPkTableName:Option[String],
                   rejTableName:String,
                   rejPkTableName:Option[String],
                   odsTableName:String
                   ) {

}
