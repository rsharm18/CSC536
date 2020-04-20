import actors.{MapActor, ReduceActor}
import akka.actor.{Actor, ActorRef, Props}
import akka.remote.routing.RemoteRouterConfig
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.{Broadcast, ConsistentHashingPool, RoundRobinPool}
import com.typesafe.config.ConfigFactory
import util.message.{AddMapActors, AddReducerActors, Done, FINISHED_MAPPING, FLUSH_OUTPUT, Flush, FlushReduce, InitActors, Init_Map, ReduceNameTitlePair, SHUTDOWN, START}

class RemoteMasterActor extends Actor {

  final case class Evict(key: String)
  var numberMappers = ConfigFactory.load.getInt("number-mappers")
  var numberReducers = ConfigFactory.load.getInt("number-reducers")

  var pendingReducers = numberReducers

  var pendingMapActors = numberMappers

  var listOfReduceActors = List[ActorRef]()

  var listOfMapActors = List[ActorRef]()

  var remoteClient = List[ActorRef]()

  var mapActorRouter = context.actorOf(Props.empty)
  var reduceActorRouter:ActorRef = context.actorOf(Props.empty)

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

  //Setup local Reducers
  for(i <- 0 until numberReducers){
    listOfReduceActors = context.actorOf(Props(classOf[ReduceActor],self), name="localreduce"+i)::listOfReduceActors
    println(listOfReduceActors.toString)
  }

  //Setup MapActors
  def setMapActors(reducers: List[ActorRef]) : Unit = {

    reducers.foreach((actor:ActorRef)=>{
      println(s"\n\n Creating local MapActors  for ${actor}")
    })
    //Setup local Mappers
    for(i <- 0 until numberMappers){
      var localMap = context.actorOf(Props(classOf[MapActor], reducers), name="localmap"+i)
     println(s"\n Created localMap ${localMap}")
      listOfMapActors = localMap::listOfMapActors

      println(listOfMapActors.toString)
    }
  }

//  def hashMapping: ConsistentHashMapping = {
//    case Evict(key) => key
//  }

  println(" ********** RemoteMasterActor is active ************")
  def receive = {


    case AddReducerActors(reducers) =>
    {

      println(s"\n ****************** received AddReducerActors from  ${sender()} ******************")

      reducers foreach((reducer:ActorRef)=>{
        listOfReduceActors = reducer :: listOfReduceActors
      })

      numberReducers = listOfReduceActors.size
      pendingReducers += reducers.size

      //set up local actors
      setMapActors(reducers)

      remoteClient = sender :: remoteClient

      //create the consistent hashing pool
      var addresses = listOfReduceActors.map(e => e.path.address).toList
      reduceActorRouter = context.actorOf(RemoteRouterConfig(ConsistentHashingPool(listOfReduceActors.size,hashMapping = hashMapping), addresses).props(Props(classOf[ReduceActor],self)),name="reduceActorRouter")


      print(s" listOfReduceActors = ${listOfReduceActors}")
      println(s"\n ****************** remoteClient are : ${remoteClient} ******************")
      sender ! AddMapActors(listOfReduceActors)
    }
    case AddMapActors(mapActors)=> {

      println(s"\n ****************** received AddMapActors from  ${sender()} ******************")

      mapActors foreach((mapper:ActorRef)=>{
        listOfMapActors = mapper :: listOfMapActors
      })

      numberMappers = listOfMapActors.size
      pendingMapActors = listOfMapActors.size

      var addresses = listOfMapActors.map(e => e.path.address).toList
      mapActorRouter = context.actorOf(RemoteRouterConfig(RoundRobinPool(numberMappers), addresses).props(Props(classOf[MapActor], listOfReduceActors)),name="mapActorRouter")

      print(s"\n no. of listOfMapActors=${listOfMapActors.size} and  listOfMapActors ${listOfMapActors} \n ")

      val numberOFFiles=ConfigFactory.load().getInt("number-source-url")

      print(s"\n ****** initiating Start *************** - Reading ${numberOFFiles} files  ")

      println(s"pendingMapActors = ${pendingMapActors} pendingReducers=${pendingReducers}")

      for(i <- 1 to numberOFFiles){
        //println("source-URL."+i)
        var entry = ConfigFactory.load().getString("source-URL."+i)
        var data = entry.split("\\|~\\|")
        //println(data(0)+" <--> "+data(data.length - 1))

        //send job to map actor
        mapActorRouter ! Init_Map(data(0),data(data.length - 1),reduceActorRouter)
      }

      Thread.sleep(2000)

      println(s"\n ****** Broadcasting Flush NOW ${sender()}***************")
      mapActorRouter ! Broadcast(Flush)
    }
    case Flush =>
      println(s"\n ****** Broadcasting Flush ${sender()}***************")
      mapActorRouter ! Broadcast(Flush)
    case FINISHED_MAPPING =>
      {
        println(s"\n *** ${sender.path} Map actor is done")
        pendingMapActors -= 1
        println(s"\n *** Waiting for ${pendingMapActors} Map actors to finish")
        if(pendingMapActors == 0)
          {
            println(s"\n *** Sending FLUSH_OUTPUT\n ")

            self ! FLUSH_OUTPUT
          }
      }
    case FLUSH_OUTPUT =>
      {
        //ask the reducers to flush their content
        reduceActorRouter ! Broadcast(FLUSH_OUTPUT)
      }
    case Done =>
      pendingReducers -= 1
      println(s"\n ==>  Received :: done  - pending = ${pendingReducers} ***************")

      if (pendingReducers == 0) {
        remoteClient.foreach((actor:ActorRef)=>{
          actor ! SHUTDOWN
        })
        context.system.terminate
      }
    case msg:String =>
      println(s"\n\n &&&& MASTER Received a test message ${msg} ${sender()}")
    case _ => println("\n |||||||||||||||||| UNKNOWN MESSAGE |||||||||||||| ")
  }
}
