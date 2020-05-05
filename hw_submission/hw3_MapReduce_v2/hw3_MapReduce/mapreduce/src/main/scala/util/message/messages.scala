package util.message

case class Init_Map(title:String,url:String)

case class ReduceNameTitlePair(name:String, title:String)

case object Flush
case object Done
