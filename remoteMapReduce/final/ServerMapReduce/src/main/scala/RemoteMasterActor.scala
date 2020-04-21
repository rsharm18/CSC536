import actors.{MapActor, ReduceActor}
import akka.actor.{Actor, ActorRef, Props}
import akka.remote.routing.RemoteRouterConfig
import akka.routing.{Broadcast, ConsistentHashingPool, RoundRobinPool}
import com.typesafe.config.ConfigFactory
import util.helper.MapperHelper
import util.message.{AddMapActors, AddReducerActors, Done, Flush, Init_Map, SHUTDOWN, START, Send_ConsistentHashRouter}

class RemoteMasterActor extends Actor {

  var numberMappers = ConfigFactory.load.getInt("number-mappers")
  var numberReducers = ConfigFactory.load.getInt("number-reducers")
  var pendingReducers = numberReducers
  var pendingMapActors = numberMappers
  var listOfReduceActors = List[ActorRef]()
  var listOfMapActors = List[ActorRef]()
  var remoteClient = List[ActorRef]()
  var mapActorRouter = context.actorOf(Props.empty)
  var reduceActorRouter: ActorRef = context.actorOf(Props.empty)

  def receive = {

    // add/register remote reducers
    case AddReducerActors(reducers) => {

      println(s"\n ****************** received AddReducerActors from  ${sender()} ******************")

      reducers foreach ((reducer: ActorRef) => {
        listOfReduceActors = reducer :: listOfReduceActors
      })

      numberReducers = listOfReduceActors.size
      pendingReducers += reducers.size

      //set up local actors with the reduce actor info
      setMapActors(reducers)

      remoteClient = sender :: remoteClient

      //create the consistent hashing pool
      var addresses = listOfReduceActors.map(e => e.path.address).toList
      reduceActorRouter = context.actorOf(RemoteRouterConfig(ConsistentHashingPool(listOfReduceActors.size, hashMapping = MapperHelper.hashMapping), addresses).props(Props(classOf[ReduceActor])), name = "reduceActorRouter")

      //println(s"\n @@@@ aissgning the reduce actor router ${reduceActorRouter}")

      MapperHelper.setReduceActorRouter(reduceActorRouter)

      // send the reduceActorRouter info to the sender
      sender ! Send_ConsistentHashRouter(reduceActorRouter)

      print(s" listOfReduceActors = ${listOfReduceActors}")
      println(s"\n ****************** remoteClient are : ${remoteClient} ******************")

      // ask sender to create the mapactors with th reducers
      sender ! AddMapActors(listOfReduceActors)
    }
    case AddMapActors(mapActors) => {

      println(s"\n ****************** received AddMapActors from  ${sender()} ******************")

      mapActors foreach ((mapper: ActorRef) => {
        listOfMapActors = mapper :: listOfMapActors
      })

      numberMappers = listOfMapActors.size
      pendingMapActors = listOfMapActors.size

      var addresses = listOfMapActors.map(e => e.path.address).toList
      mapActorRouter = context.actorOf(RemoteRouterConfig(RoundRobinPool(numberMappers), addresses).props(Props(classOf[MapActor], listOfReduceActors)), name = "mapActorRouter")

      self ! START
    }

    case START => {

      println(s"\n ****** initiating MAPREDUCE *************** ");

      println(s"\n no. of listOfMapActors=${listOfMapActors.size} and  listOfMapActors ${listOfMapActors}")

      //read the number of files to be read
      val numberOFFiles = ConfigFactory.load().getInt("number-source-url")

      println(s"\n\n ******  Reading ${numberOFFiles} files  \n\n")

      //println(s"pendingMapActors = ${pendingMapActors} pendingReducers=${pendingReducers}")

      for (i <- 1 to numberOFFiles) {

        var entry = ConfigFactory.load().getString("source-URL." + i)

        // Data = "Title"|~|"URL"
        var data = entry.split("\\|~\\|")
        //println(data(0)+" <--> "+data(data.length - 1))

        //send job to map actor
        mapActorRouter ! Init_Map(title = data(0), url = data(data.length - 1))
      }
      //allow the actors to read file
      Thread.sleep(1000)

      //ask everyone to flush
      self ! Flush
    }
    case Flush =>
      println(s"\n ****** Broadcasting Flush ***************")
      mapActorRouter ! Broadcast(Flush)

    case Done =>
      pendingReducers -= 1
      println(s"\n ==>  Received :: done from ${sender} - pending Done message from ${pendingReducers} reducers ***************")

      if (pendingReducers == 0) {
        remoteClient.foreach((actor: ActorRef) => {
          actor ! SHUTDOWN
        })
        println("\n\n  +++ Please check the root directories of the Server and  the Client Map actor for the generated output +++")
        println("\n\n  +++ Terminating. Good Bye Now!\n\n")

        context.system.terminate
      }
    case msg: String =>
      println(s"\n\n &&&& MASTER Received a test message ${msg} ${sender()}")

    case _ => println("\n |||||||||||||||||| UNKNOWN MESSAGE |||||||||||||| ")
  }

  MapperHelper.setContextMasterActor(self)

  //Setup local Reducers
  for (i <- 0 until numberReducers) {
    listOfReduceActors = context.actorOf(Props(classOf[ReduceActor]), name = "localreduce" + i) :: listOfReduceActors
    println(listOfReduceActors.toString)
  }

  //Setup MapActors
  def setMapActors(reducers: List[ActorRef]): Unit = {

    reducers.foreach((actor: ActorRef) => {
      println(s"\n\n Creating local MapActors  for ${actor}")
    })
    //Setup local Mappers
    for (i <- 0 until numberMappers) {
      var localMap = context.actorOf(Props(classOf[MapActor], reducers), name = "localmap" + i)
      println(s"\n Created localMap ${localMap}")
      listOfMapActors = localMap :: listOfMapActors

      println(listOfMapActors.toString)
    }
  }


  println(" ********** RemoteMasterActor is active ************")

}
