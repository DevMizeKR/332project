package project332.master

import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.CountDownLatch
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}

import project332.common.Common.{findRandomPort, getLocalIP}
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}

object Master extends LazyLogging {

  private val randomPort: Int = findRandomPort

  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty) return

    val port = args.find(arg => arg == "DEBUG").map(_ => 50051).getOrElse(randomPort)
    val server = new Master(ExecutionContext.global, args.headOption.get.toInt, port)
    server.start()
    server.printEndPoint()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext, val numClient: Int, val port: Int) extends LazyLogging {
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  private var clients: Map[String, String] = Map()

  // 서버 시작
  def start(): Unit = {
    server = ServerBuilder.forPort(this.port)
      .addService(CommunicateGrpc.bindService(new CommunicateImpl, executionContext))
      .build()
      .start()

    Master.logger.info(s"Server started, listening on ${this.port}")

    sys.addShutdownHook {
      stop()
      Master.logger.warn("Server shut down")
    }
  }

  private def printEndPoint(): Unit = {
    System.out.println(getLocalIP + " : " + this.port)
  }

  // 서버 종료
  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  // 서버 종료 대기
  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  // gRPC 서비스 구현
  private class CommunicateImpl extends CommunicateGrpc.Communicate {
    override def connecting(req: ConnectionRequest): Future[ConnectionResponse] = {
      Master.logger.info(s"Handshake from ${req.ipAddress}")
      clientLatch.countDown()
      clientLatch.await()

      val reply = ConnectionResponse(isConnected = true, id = clientLatch.getCount.toInt)
      Future.successful(reply)
    }
  }
}