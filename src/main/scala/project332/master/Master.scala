package project332.master

import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString

import java.util.concurrent.CountDownLatch
import io.grpc.{Server, ServerBuilder}

import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import project332.common.States._
import project332.common.Common.{findRandomPort, getLocalIP}
import project332.common.KeyOrdering
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}
import project332.connection.{SamplingRequest, SamplingResponse}
import project332.connection.SamplingResponse.KeyRange

object Master extends LazyLogging {

  private val randomPort: Int = findRandomPort

  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty) {
      println("Usage: ./master <Client Number>")
      System.exit(1)
    }

    val port = args.find(arg => arg == "DEBUG").map(_ => 50052).getOrElse(randomPort)
    val server = new Master(ExecutionContext.global, args.headOption.get.toInt, port)
    server.start()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext, val numClient: Int, val port: Int) extends LazyLogging {
  private[this] var server: Server = _
  private var clientLatch: CountDownLatch = _
  private var workers: Vector[WorkerClient] = Vector.empty
  var currentState: State = InitialState
  var sampleData: List[Array[Byte]] = Nil
  var idKeyRange: Map[Int, KeyRange] = Map.empty
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

    initialTransition(ConnectingState)
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

  // Add New Worker to Master
  private def addWorker(ipAddress: String): Int = {
    this.synchronized {
      val workerID = this.workers.length
      this.workers = this.workers :+ new WorkerClient(this.workers.length, ipAddress)
      if (this.workers.length == this.numClient) {
        Master.logger.info(s"${this.workers.mkString(", ")}")
        Future { stateTransition(SamplingState) }
      }
      return workerID
    }
  }

  private def initialTransition(nextState: State): Unit = {
    Master.logger.info(s"Transition to $nextState.")
    currentState = nextState
    clientLatch = new CountDownLatch(numClient)
  }

  private def stateTransition(nextState: State): Unit = {
    Master.logger.info(s"Transition to $nextState.")
    currentState = nextState
    clientLatch.await()
    clientLatch = new CountDownLatch(numClient)
  }

  // Add Sampled Data from Worker
  private def addSampleData(id: Int, sampleData : Array[Byte]): Unit = {
    this.synchronized {
      val worker = this.workers.find(_.id == id).get
      worker.gotData = true

      this.sampleData = this.sampleData ++ sampleData.grouped(10).toList
      Master.logger.info(s"Received Data with length ${this.sampleData.length}.")
      if (this.workers.count(_.gotData) == numClient) {
        Master.logger.info(s"Received All Data from clients.")
        createPivot()
        Future { stateTransition(SortingState) }
      }
    }
  }

  // Create Pivot for Partitioning
  private def createPivot(): Unit = {
    val minData: Array[Byte] = Array(Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue)
    val maxData: Array[Byte] = Array(Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue)
    val sortedData = this.sampleData.sorted(KeyOrdering)
    val partition: Map[Int, (Array[Byte], Array[Byte])] = Map.empty
    if (this.workers.length == 1) { partition.put(workers(0).id, (minData, maxData))}
    else {
      val range = sortedData.length / this.workers.length
      var loop = 0

      for (worker <- workers.toList) {
        val startIndex = loop * range
        val endIndex = Math.min((loop + 1) * range, sortedData.length)

        if (loop == 0) {
          val bytes = sortedData(endIndex - 1).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(worker.id, (minData, bytes))
        } else if (loop == this.workers.length - 1 || endIndex == sortedData.length) {
          partition.put(worker.id, (sortedData(startIndex), maxData))
        } else {
          val bytes = sortedData(endIndex - 1).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(worker.id, (sortedData(startIndex), bytes))
        }
        loop += 1
      }
    }

    idKeyRange = partition.map(x=>(x._1, KeyRange(lowerBound = ByteString.copyFrom(x._2._1), upperBound = ByteString.copyFrom(x._2._2))))
    Master.logger.info(s"Check Key Range\n $idKeyRange")
  }

  // gRPC 서비스 구현
  private class CommunicateImpl extends CommunicateGrpc.Communicate {
    override def connecting(req: ConnectionRequest): Future[ConnectionResponse] = {
      Master.logger.info(s"Handshake from ${req.ipAddress}")

      val workerID = addWorker(req.ipAddress)
      clientLatch.countDown()
      clientLatch.await()

      val reply = ConnectionResponse(isConnected = true, id = workerID)
      Future.successful(reply)
    }

    override def sampling(req: SamplingRequest): Future[SamplingResponse] = {
      addSampleData(req.id, req.data.toByteArray)
      clientLatch.countDown()
      clientLatch.await()

      val reply = SamplingResponse(isChecked = true, idKeyRange = idKeyRange.toMap)
      Future.successful(reply)
    }
  }
}

class WorkerClient(val id: Int, val ip: String) {
  var gotData: Boolean = false
  override def toString: String = ip
}