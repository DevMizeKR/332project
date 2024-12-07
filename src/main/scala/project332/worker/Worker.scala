package project332.worker

import java.util.concurrent.TimeUnit
import java.net._
import com.typesafe.scalalogging.LazyLogging
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import project332.connection.{InitialConnectGrpc, ConnectionRequest, ConnectionReply}
import project332.sorting.{SortingGrpc, SortingRequest, SortingReply}

object Worker {
  def main(args: Array[String]): Unit = {
    val client = Worker("127.0.0.1", 50052)
    try {
      val user = args.headOption.getOrElse("world")
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIPAddress: String = localhost.getHostAddress
      client.sendClientIP(user, localIPAddress)
    } finally {
      client.shutdown()
    }
  }

  def apply(host: String, port: Int): Worker = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
    val blockingStub = InitialConnectGrpc.blockingStub(channel)
    new Worker(channel, blockingStub)
  }

  def getLocalIP: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
}

class Worker private(
                      private val channel: ManagedChannel,
                      private val blockingStub: InitialConnectGrpc.InitialConnectBlockingStub
                    ) extends LazyLogging {

  // shutdown 메서드: gRPC 채널 종료
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  // Send Request to Server and Get Reply
  def sendClientIP(name: String, ip: String): Unit = {
    logger.info(s"Will try to greet $name ...")
    val request = ConnectionRequest(ipAddress = ip, name = name)
    try {
      val response = blockingStub.makeIPConnect(request)
      logger.info(s"Greeting: ${response.message}")
    } catch {
      case e: StatusRuntimeException =>
        logger.warn("RPC failed: " + e.getStatus)
    }
  }
}