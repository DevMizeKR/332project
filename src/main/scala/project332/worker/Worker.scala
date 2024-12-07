package project332.worker

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import project332.common.Common.getLocalIP
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}

object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: Worker <master-ip> <master-port>")
      System.exit(1)
    }

    val masterIp = args(0)
    val masterPort = args(1).toInt

    val worker = new Worker(masterIp, masterPort)
    val done = worker.start()
    Await.result(done, Duration.Inf)
    worker.shutdown()
  }
}

class Worker(masterIp: String, masterPort: Int) extends LazyLogging {
  private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(masterIp, masterPort)
    .usePlaintext()
    .build()
  private val stub: CommunicateGrpc.CommunicateStub = CommunicateGrpc.stub(channel)
  private val done: Promise[Boolean] = Promise[Boolean]()
  private var id: Option[Int] = None

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, SECONDS)
  }

  def start(): Future[Boolean] = {
    connectToMaster()
    done.future
  }

  private def connectToMaster(): Unit = {
    val request = ConnectionRequest(ipAddress = getLocalIP)
    val response: Future[ConnectionResponse] = stub.connecting(request)

    response.onComplete {
      case Success(res) =>
        handleConnectionResponse(res)
      case Failure(exception) =>
        logger.error(s"Connection to master failed: ${exception.getMessage}")
        done.failure(exception)
    }
  }

  private def handleConnectionResponse(response: ConnectionResponse): Unit = {
    if (response.isConnected) {
      this.id = Some(response.id)
      logger.info(s"Connected to master with ID: ${response.id}")
      done.success(true)
    } else {
      logger.error("Connection to master was rejected.")
      done.failure(new Exception("Connection rejected"))
    }
  }
}