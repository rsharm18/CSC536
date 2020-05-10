class Stats {
  var messages: Int = 0
   var sent_multicasts: Int = 0
  var received_multicasts: Int = 0
  var received_multicast_errors: Int = 0

  var joinedNewGroup:Int = 0
  var leaveGroup:Int = 0

  def += (right: Stats): Stats = {
    // total number of commands
    messages += right.messages

    // Multicast info
    sent_multicasts += right.sent_multicasts
    received_multicasts += right.received_multicasts

    received_multicast_errors += right.received_multicast_errors

    // group info
    joinedNewGroup +=right.joinedNewGroup
    leaveGroup +=right.leaveGroup

    this
  }

  override def toString(): String = {
    s"Stats msgs=$messages #Groups_Joined=$joinedNewGroup #Groups_Left=$leaveGroup #sent_multicasts=$sent_multicasts #received_multicasts=$received_multicasts #Messages_received_after_leaving_group=$received_multicast_errors"
  }
}
