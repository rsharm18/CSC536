import java.util.Date

import actors.{MapActor, ReduceActor}
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

  object RemoteReduceActor extends  App{

    val system = ActorSystem("RemoteMapReduce", ConfigFactory.load.getConfig("remotelookup"))

    val remoteMapActor = system.actorOf(Props[ReduceActor])

    println(s"\n\n ********** Remote Map Ready ${remoteMapActor.path} at port=${remoteMapActor.path.address.port} ********** ")

}
