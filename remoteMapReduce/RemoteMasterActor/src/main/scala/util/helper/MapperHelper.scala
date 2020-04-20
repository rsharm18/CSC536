package util.helper

import java.io.{FileNotFoundException, IOException}

import scala.collection.mutable
import scala.io.Source

object MapperHelper {



  def readFromFile(url:String): String =
  {
//    val strBuilder =new  StringBuilder();
//    try {
//      Source.fromURL(url).getLines().foreach((f: String) => {
//        strBuilder.append(f)
//      }
//      )
//    }
//    catch
//      {
//        case e:FileNotFoundException => e.printStackTrace()
//        case e:IOException =>e.printStackTrace()
//        case _:Throwable =>println("Unknown Error in reading file")
//      }
//    println(s"\n\n *** Finished Reading file from ${url} length ${strBuilder.size}")
//
//    strBuilder.toString()

    Source.fromURL(url).mkString
  }


  def extractProperName(title:String,content: String):mutable.HashMap[String,String] = {
    var nameTitlePair = new mutable.HashMap[String,String]()
    for (word <- content.split(" "))
    {

      //get Proper case
      //if(!isAllDigits(word.trim) && word.trim.length>0 && word.trim.toUpperCase().equals(word.trim))
      if(word.trim.matches("\\b[A-Z][a-z]*\\b"))
      {
        nameTitlePair put (word.trim, title)
      }
    }
    nameTitlePair
  }

  def isAllDigits(x: String) = {
    var isNum=false
    x forall(y => {
      if (y.isDigit) {
        isNum = true
      }
      isNum
    })
  }

}
