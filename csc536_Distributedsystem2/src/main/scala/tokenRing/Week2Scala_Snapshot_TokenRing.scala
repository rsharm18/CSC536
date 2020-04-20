import tokenRing.chandy_lamport_snapshot.ChandyLamportSnapshot
import tokenRing.pingPong.TokenRingPingPong

object Server extends App {

  // Set disableInput=false to chose between token ring and chandy lamport algo
  var disableInput = true

  if (!disableInput) {

    println(" Please enter your option")
    println(" Enter 1 for Ping Pong")
    println(" Enter 2 for Chandy Lamport Algorithm");

    print("Enter your input ")
    var i: Int = scala.io.StdIn.readInt()

    if (i == 1) {
      TokenRingPingPong.initTokenRing()
    }
    else {
      ChandyLamportSnapshot.initChandyLamport();
    }
  }
  else { //Default run the Chandy Lamport algorithm
    ChandyLamportSnapshot.initChandyLamport();
  }

}