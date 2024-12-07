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
