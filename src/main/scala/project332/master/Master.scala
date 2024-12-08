package project332.master

import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.CountDownLatch
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}

import project332.common.Common.{findRandomPort, getLocalIP}
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}
import project332.connection.{SamplingRequest, SamplingResponse}

object Master extends LazyLogging {

  private val randomPort: Int = findRandomPort

  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty) return

    val port = args.find(arg => arg == "DEBUG").map(_ => 50052).getOrElse(randomPort)
    val server = new Master(ExecutionContext.global, args.headOption.get.toInt, port)
    server.start()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext, val numClient: Int, val port: Int) extends LazyLogging {
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  private var workers: Vector[WorkerClient] = Vector.empty
  var data: List[Array[Byte]] = Nil
  var count: Int = 0

  // 서버 시작
  private def start(): Unit = {
    server = ServerBuilder.forPort(this.port)
      .addService(CommunicateGrpc.bindService(new CommunicateImpl, executionContext))
      .build()
      .start()

    Master.logger.info(s"Client Number : $numClient")
    Master.logger.info(s"Server started at $getLocalIP:${this.port}")

    sys.addShutdownHook {
      stop()
      Master.logger.warn("Server shut down")
    }
  }

  // 서버 종료
  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  // 서버 종료 대기
  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def addData(data: String): Unit = {
    this.synchronized {
      count += 1
      this.data :+ data.getBytes().grouped(10).toList
      if (count == numClient)
        Master.logger.info(s"Received All Data from $count clients.")
    }
  }

  private def addWorker(ipAddress: String): Unit = {
    this.synchronized {
      this.workers = this.workers :+ new WorkerClient(this.workers.length, ipAddress)
      if (this.workers.length == this.numClient)
        Master.logger.info(s"${this.workers.mkString(", ")}")
    }
  }

  // gRPC 서비스 구현
  private class CommunicateImpl extends CommunicateGrpc.Communicate {
    override def connecting(req: ConnectionRequest): Future[ConnectionResponse] = {
      Master.logger.info(s"Handshake from ${req.ipAddress}")
      clientLatch.countDown()
      addWorker(req.ipAddress)
      clientLatch.await()

      val reply = ConnectionResponse(isConnected = true, id = clientLatch.getCount.toInt)
      Future.successful(reply)
    }

    override def sampling(req: SamplingRequest): Future[SamplingResponse] = {
      Master.logger.info(s"Data from ${req.ipAddress}")
      clientLatch.countDown()
      addData(req.data)
      clientLatch.await()

      val reply = SamplingResponse(isChecked = true)
      Future.successful(reply)
    }
  }
}

class WorkerClient(val id: Int, val ip: String) {
  override def toString: String = ip
}