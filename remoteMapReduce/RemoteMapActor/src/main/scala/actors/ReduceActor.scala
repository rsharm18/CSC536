package actors

import akka.actor.{Actor, ActorRef}
import util.message.{Done, FLUSH_OUTPUT, Flush, FlushReduce, ReduceNameTitlePair}

import scala.collection.mutable.HashMap

class ReduceActor(masterActor:ActorRef) extends Actor {
  var remainingMappers = 4 //ConfigFactory.load.getInt("number-mappers")
  var reduceMap = HashMap[String, Int]()

  var reduceMapTitle = HashMap[String, List[String]]()

  var iCOunt=1


  def receive = {
    case ReduceNameTitlePair(name,title) =>
      {
        produceNameTitleData(name,title)
      }
    case Flush =>
      remainingMappers -= 1
      iCOunt = iCOunt +1
      println(s"\n\n ~~~~~~~~  Sender - ${sender.path}  -- self = ${self.path} - remainingMappers? ${remainingMappers} received flush - iCOunt ${iCOunt}")
      if (remainingMappers == 0) {
        var stringBuilder = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toStringWithoutAddress} :: START ***\n")
        //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
        //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
        reduceMapTitle.foreach((data:(String,List[String]))=>{

          var iCount = data._2.size
          stringBuilder.append("(").append(data._1).append(",[")
          data._2.foreach((title:String)=>{
            stringBuilder.append(title)
            if(iCount>1){
              stringBuilder.append(",")
            }
            iCount -=1

          })
          stringBuilder.append("]").append(")  ")
        })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
        //println(stringBuilder.toString)

        println(s" @@@@@ Sending DONE to ${context.parent}")
        context.parent ! Done
      }
    case FlushReduce(msg) =>

      remainingMappers -= 1
      iCOunt = iCOunt +1
      //println(s"\n\n ~~~~~~~~  Sender - ${sender.path}  -- self = ${self.path} - remainingMappers? ${remainingMappers} received flush - iCOunt ${iCOunt}")
      if (remainingMappers == 0) {
        var stringBuilder = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toStringWithoutAddress} :: START ***\n")
        //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
        //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
        reduceMapTitle.foreach((data:(String,List[String]))=>{

          var iCount = data._2.size
          stringBuilder.append("(").append(data._1).append(",[")
          data._2.foreach((title:String)=>{
            stringBuilder.append(title)
            if(iCount>1){
              stringBuilder.append(",")
            }
            iCount -=1

          })
          stringBuilder.append("]").append(")  ")
        })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END ***\n")
        println(stringBuilder.toString)

        //println(s" @@@@@ Sending DONE to ${context.parent}")
        masterActor ! Done
      }
    case FLUSH_OUTPUT =>
      {
        var stringBuilder = new StringBuilder();

        stringBuilder
          .append(s"\n *** ${self.path.toString} :: START - FLUSH_OUTPUT***\n")
        //.append(s"\n \t ${self.path.toStringWithoutAddress} :: Proper Name(Count) : ${reduceMap.toString()}")
        //.append(s"\n\n \t ${self.path.toStringWithoutAddress} :: Word(Title)        : ${reduceMapTitle.toString()}")
//          reduceMapTitle.foreach((data:(String,List[String]))=>{
//
//            var iCount = data._2.size
//            stringBuilder.append("(").append(data._1).append(",[")
//            data._2.foreach((title:String)=>{
//             stringBuilder.append(title)
//              if(iCount>1){
//                stringBuilder.append(",")
//              }
//              iCount -=1
//
//            })
//            stringBuilder.append("]").append(")  ")
//          })
        stringBuilder.append(s"\n\n **** ${self.path.toStringWithoutAddress} :: END 0 FLUSH_OUTPUT ***\n")
        println(stringBuilder.toString)

        sender ! Done
      }
  }

  def produceNameTitleData(word:String,title:String) = {
    //println(s"\n ${self.path.name} : processing the NAME TITLE data")
    if (reduceMap.contains(word))
    {
      reduceMap += (word -> (reduceMap(word) + 1))
      reduceMapTitle put ( word,  title :: reduceMapTitle(word))
    }
    else {
      reduceMap += (word -> 1)
      reduceMapTitle.put( word,List(title))
    }
//    if(reduceMapTitle.size != 0)
//      print(s"\t ${self.path.name} : processing the NAME TITLE data ${reduceMapTitle.size}")
  }
}
