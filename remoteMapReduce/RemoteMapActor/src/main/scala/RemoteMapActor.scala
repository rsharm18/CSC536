import actors.{MapActor, ReduceActor}
import akka.actor.{Actor, ActorRef, Props}
import akka.remote.routing.RemoteRouterConfig
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.RoundRobinPool
import com.typesafe.config.ConfigFactory
import util.message.{AddMapActors, AddReducerActors, Done, FlushReduce, Init_Map, ReduceNameTitlePair, SHUTDOWN, START}

  class RemoteMapActor extends  Actor {

    val numberMappers  = ConfigFactory.load.getInt("number-mappers")
    val numberReducers  = ConfigFactory.load.getInt("number-reducers")
    var pending = numberReducers

    val master = context.actorSelection("akka://RemoteMapReduce@127.0.0.1:25535/user/masterActor")

    var listReduceActors:List[ActorRef] = List[ActorRef]()
    var listOfMapActors:List[ActorRef] = List[ActorRef]()


    //create the remote reducers
    for (i <- 0 until numberReducers)
      listReduceActors = context.actorOf(Props(classOf[ReduceActor],self), name = "RemoteReduce"+i)::listReduceActors

    //send the remote reduce actors to Master
    master ! AddReducerActors(listReduceActors)

    //Setup MapActors
    def setMapActors(reducers: List[ActorRef]) : Unit = {

      //Setup remote Mappers
      for(i <- 0 until numberMappers){
        var localMap = context.actorOf(Props(classOf[MapActor], reducers), name="RemoteMap"+i)
        //println(s"\n Created localMap ${localMap}")

        listOfMapActors = localMap::listOfMapActors

        //println(listOfMapActors.toString)
      }
    }

    def hashMapping: ConsistentHashMapping = {
      case ReduceNameTitlePair(name,title) => {
        //println(s"\n *** Using ${name} as the key")
        name
      }
      case FlushReduce(msg) =>
      {
        msg.hashCode
      }
    }
    override def receive = {

      case AddMapActors(reducers:List[ActorRef]) =>
      {

        println(s"\n ********** received add map actor from ${sender()} ***************")

        setMapActors(reducers)
        sender! AddMapActors(listOfMapActors)

      }
      case Done =>
        println(s"\n ===================> ${sender.path }  is done. Forwarding the info to server.")
        master ! Done

      case SHUTDOWN =>{
        println(s"\n ********** I received shutdown from Sender ${sender()} ***************")
        Thread.sleep(1000)
          context.system.terminate()
      }
      case _ => print("\n ^^^^^^^^^^^^^ UNKNOWN MESSAGE 1 ^^^^^^^^^^^^^^^^^^ ")

    }
  }
