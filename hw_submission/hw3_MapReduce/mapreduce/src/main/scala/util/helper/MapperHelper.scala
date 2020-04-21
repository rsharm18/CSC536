package util.helper

import java.io.{FileNotFoundException, FileWriter, IOException}

import scala.collection.mutable
import scala.io.Source

object MapperHelper {



  def readFromFile(url: String): String = {

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

    try {
      Source.fromURL(url.trim).mkString
    }
    catch {
      case e: Exception => {
        println(s"Sorry, Could not read the file from:${url}-- due to the exception \n")
        e.printStackTrace()

        "" //return empty data
      }
    }


  }


  def extractProperName(title: String, content: String): mutable.HashMap[String, String] = {
    var nameTitlePair = new mutable.HashMap[String, String]()

    //split the word by space
    for (word <- content.split("\\s")) {
      //get Proper case - work starting with upper case and ends with lower case
      if (word.trim.matches("^[A-Z][a-z]+$")){

        nameTitlePair put (word.trim, title)
      }
    }
    nameTitlePair
  }



  // write the data to file
  def writeOutputToFile(actorName:String,data:String): Unit =
  {
    try {
      var fw = new FileWriter(s"${actorName}.txt", false);
      fw.write(data);
    }
    catch
      {
        case e:Exception => {
          print(s"Error creating/writing to the file ${actorName}.txt. Error Message = ")
          e.printStackTrace()
        }
      }

  }

}
