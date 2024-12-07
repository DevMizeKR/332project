package project332.master

import com.typesafe.scalalogging.LazyLogging
import java.net._
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import project332.connection.{ConnectionRequest, ConnectionReply, InitialConnectGrpc}
import project332.sorting.{SortingGrpc, SortingRequest, SortingReply}

object Master extends LazyLogging {

  private val port = 50051

  def main(args: Array[String]): Unit = {
    Master.logger.info("Server started")
    val server = new Master(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  def getLocalIP: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
}

class Master(executionContext: ExecutionContext) extends LazyLogging {
  private[this] var server: Server = null
  private var clients: Map[String, String] = Map()

  // 서버 시작
  def start(): Unit = {
    server = ServerBuilder
      .forPort(Master.port) // 포트 지정
      .addService(InitialConnectGrpc.bindService(new ConnectionImpl, executionContext))
      .build()
      .start()

    Master.logger.info("Server started, listening on ${Master.port}")

    sys.addShutdownHook {
      Master.logger.warn("*** shutting down gRPC server since JVM is shutting down")
      stop()
      Master.logger.warn("*** server shut down")
    }
  }

  // 서버 종료
  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
      Master.logger.info("Server stopped.")
    }
  }

  // 서버 종료 대기
  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  // gRPC 서비스 구현
  private class ConnectionImpl extends InitialConnectGrpc.InitialConnect {
    override def makeIPConnect(req: ConnectionRequest): Future[ConnectionReply] = {
      val name = req.name
      val reply = ConnectionReply(message = "Hello" + name)

      Master.logger.info(s"Connected with : $name")
      Master.logger.info(s"IP Address : $name")

      clients = clients + (req.name -> req.ipAddress)
      Future.successful(reply)
    }
  }
}