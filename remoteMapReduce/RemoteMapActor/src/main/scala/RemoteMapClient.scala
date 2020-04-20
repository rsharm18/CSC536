import actors.MapActor
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object RemoteMapClient extends  App{

  val system = ActorSystem("RemoteMapReduce", ConfigFactory.load.getConfig("remotelookup"))


  val remoteMapActor = system.actorOf(Props[RemoteMapActor], name="RemoteJVM")

  println(s"\n\n ******** Remote Map Ready ${remoteMapActor.path}  at port=${remoteMapActor.path.address.port} ******** ")

}