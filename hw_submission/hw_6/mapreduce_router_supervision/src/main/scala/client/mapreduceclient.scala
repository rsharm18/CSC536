package client

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorSystem, Props}

import common._

object MapReduceClient extends App {

  val system = ActorSystem("MapReduceClient", ConfigFactory.load.getConfig("client"))

  val master = system.actorOf(Props[MasterActor], name = "master")

  master ! Book("A Tale of Two Cities", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg98.txt")
 
  //below URL is invalid
  master ! Book("The Pickwick Papers", "http:// reed.cs.depaul.edu/lperkovic/gutenberg/pg580.txt")
  
  master ! Book("A Child's History of England", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg699.txt")
  master ! Book("The Old Curiosity Shop", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg700.txt")
  master ! Book("Oliver Twist", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg730.txt")
  master ! Book("David Copperfield", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg766.txt")
  master ! Book("Hunted Down", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg807.txt")
  master ! Book("Dombey and Son", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg821.txt")
  master ! Book("Our Mutual Friend", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg883.txt")
  master ! Book("Little Dorrit", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg963.txt")
  master ! Book("Life And Adventures Of Martin Chuzzlewit", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg967.txt")
  master ! Book("The Life And Adventures Of Nicholas Nickleby", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg968.txt")
  master ! Book("Bleak House", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg1023.txt")
  master ! Book("Great Expectations", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg1400.txt")
  master ! Book("A Christmas Carol", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg19337.txt")
  master ! Book("The Cricket on the Hearth", "http://reed.cs.depaul.edu/lperkovic/gutenberg/pg20795.txt")

  master ! Flush
}
