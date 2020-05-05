import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory


object Server extends App {

  val system = ActorSystem("RemoteMapReduce", ConfigFactory.load.getConfig("server"))
  println("Server ready")

  val master = system.actorOf(Props[RemoteMasterActor], name = "masterActor")
  println(master.path)
  //system.terminate
}
