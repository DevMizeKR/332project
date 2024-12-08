package project332.common

import java.net._

object Common {
  def getLocalIP: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }

  def findRandomPort: Int = {
    val socket = new ServerSocket(0)
    val availablePort = socket.getLocalPort
    socket.close()
    availablePort
  }
}

object KeyOrdering extends Ordering[Array[Byte]] {
  override def compare(x: Array[Byte], y: Array[Byte]): Int = {
    assert(x.length == y.length)
    for (i <- 0 to 9) {
      if (x(i) > y(i)) return 1
      else if (x(i) < y(i)) return -1
    }
    0
  }
}

object States extends Enumeration {
  type State = Value
  val InitialState, ConnectingState, SamplingState, SortingState, ShufflingState, MergingState, EndState = Value
}