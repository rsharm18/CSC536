package common

trait Message
case class Book(title: String, url: String) extends Message
case class Word(word:String, title: String) extends Message
case object Flush extends Message
case object Done extends Message
