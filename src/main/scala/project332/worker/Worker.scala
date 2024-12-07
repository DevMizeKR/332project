package project332.worker

import java.util.concurrent.TimeUnit
import java.net._
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import project332.connection.{ConnectionReply, ConnectionRequest, InitialConnectGrpc}
import com.typesafe.scalalogging.LazyLogging

object Worker {
  def apply(host: String, port: Int): Worker = {
    val channel = ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build()
    val blockingStub = InitialConnectGrpc.blockingStub(channel)
    new Worker(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = Worker("127.0.0.1", 50051)
    try {
      val user = args.headOption.getOrElse("world")
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIPAddress: String = localhost.getHostAddress
      client.sendClientIP(user, localIPAddress)
    } finally {
      client.shutdown()
    }
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

  // greet 메서드: 서버로 요청을 보내고 응답을 받아오는 메서드
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