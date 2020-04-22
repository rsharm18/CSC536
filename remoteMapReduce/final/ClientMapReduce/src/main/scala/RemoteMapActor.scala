import actors.{MapActor, ReduceActor}
import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.ConfigFactory
import util.helper.MapperHelper
import util.message.{AddMapActors, AddReducerActors, Done, SHUTDOWN, Send_ConsistentHashRouter}

class RemoteMapActor extends Actor {

  val numberMappers = ConfigFactory.load.getInt("number-mappers")
  val numberReducers = ConfigFactory.load.getInt("number-reducers")
  val master = context.actorSelection("akka://RemoteMapReduce@127.0.0.1:25535/user/masterActor")
  var pending = numberReducers
  var listReduceActors: List[ActorRef] = List[ActorRef]()
  var listOfMapActors: List[ActorRef] = List[ActorRef]()

  val helper = MapperHelper

  helper.setContextMasterActor(self)
  //create the remote reducers
  for (i <- 0 until numberReducers)
    listReduceActors = context.actorOf(Props(classOf[ReduceActor]), name = "RemoteReduce" + i) :: listReduceActors

  //send the remote reduce actors to Master
  master ! AddReducerActors(listReduceActors)

  override def receive = {

    case AddMapActors(reducers: List[ActorRef]) => {

      println(s"\n ********** received add map actor from ${sender()} ***************")

      setMapActors(reducers)
      sender ! AddMapActors(listOfMapActors)

    }
    case Send_ConsistentHashRouter(reduceActorRouter: ActorRef) => {
      helper.setReduceActorRouter(reduceActorRouter)
    }
    case Done =>
      println(s"\n ===================> ${sender.path}  is done. Forwarding the info to server.")
      master ! Done

    case SHUTDOWN => {
      println(s"\n ********** I received shutdown from Sender ${sender()} ***************")
      Thread.sleep(1000)

      println("\n\n  +++ Please check the root directories of the Server and  the Client Map actor for the generated output +++")
      println("\n\n  +++ Terminating. Good Bye Now!\n\n")

      context.system.terminate()
    }
    case _ => print("\n ^^^^^^^^^^^^^ UNKNOWN MESSAGE 1 ^^^^^^^^^^^^^^^^^^ ")

  }

  //Setup MapActors
  def setMapActors(reducers: List[ActorRef]): Unit = {

    //Setup remote Mappers
    for (i <- 0 until numberMappers) {
      var localMap = context.actorOf(Props(classOf[MapActor], reducers), name = "RemoteMap" + i)
      //println(s"\n Created localMap ${localMap}")

      listOfMapActors = localMap :: listOfMapActors

      //println(listOfMapActors.toString)
    }
  }
}
