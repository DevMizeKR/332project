package project332.worker

import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}

import java.io.{File, FileOutputStream}
import java.util.concurrent.CountDownLatch
import scala.io.Source
import scala.util.{Failure, Success, Random}
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global
import project332.common.Common.{getLocalIP, findRandomPort}
import project332.common.KeyOrdering
import project332.connection.{CommunicateGrpc, ConnectionRequest, SamplingRequest}
import project332.connection.{ShufflingServerRequest, ShufflingCompletedRequest}
import project332.connection.{ShuffleGrpc, ShufflingRequest, ShufflingResponse}
import project332.connection.{MergingCompletedRequest, MergingCompletedResponse}
import project332.connection.SamplingResponse.KeyRange

object Worker extends LazyLogging {
  def apply(ip: String, port: Int, inputDirectories: Array[String], outputDirectory: String): Worker = {
    val channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
    val stub = CommunicateGrpc.stub(channel)
    new Worker(channel, stub, inputDirectories, outputDirectory)
  }

  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty) {
      println("Usage: ./worker <Master IP>:<Master Port> <Input Number> <Input Directory Lists> <Output Directory>")
      System.exit(1)
    }

    val masterIP = args.headOption.get.split(':')
    val inputCount = args(1).toInt
    val inputDirectories: Array[String] = args.slice(2, 2 + inputCount)
    val outputDirectory = args.lastOption.get

    Worker.logger.info(s"Try to connect with Master : ${masterIP(0)}")


    val client = Worker(masterIP(0), masterIP(1).toInt, inputDirectories, outputDirectory)
    val done = client.start()
    Await.result(done, Duration.Inf)
    client.shutdown()
  }
}

class Worker(private val channel: ManagedChannel,
             private val stub: CommunicateGrpc.CommunicateStub,
             private val inputDirectories: Array[String],
             private val outputDirectory: String
            ) extends LazyLogging {
  val done: Promise[Boolean] = Promise[Boolean]()
  var idKeyRange: mutable.Map[Int, KeyRange] = mutable.Map.empty
  var idAddress: Map[Int, String] = Map.empty
  var workerServer: Server = _
  var serverPort: Int = 0
  var id: Int = 0

  def start(): Future[Boolean] = {
    Future { initialConnect() }
    done.future
  }

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, SECONDS)
  }

  private def initialConnect(): Unit = {
    val request = ConnectionRequest(ipAddress = getLocalIP)
    val response = stub.connecting(request)
    response.onComplete {
      case Success(value) =>
        Worker.logger.info(s"Connection : ${value.isConnected}")
        Worker.logger.info(s"Worker ID : ${value.id}")
        this.id = value.id
        val sampleData = getSampleData
        sendSampleData(sampleData)
      case Failure(exception) => Worker.logger.warn(s"Failed : $exception")
    }
    Await.ready(response, 10.seconds)
  }

  private def getSampleData: Array[Byte] = {
    val data = inputDirectories.map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .map(Source.fromFile(_))
      .flatMap(_.grouped(100).toList.map(x => x.dropRight(90)).take(10000).flatMap(x => x.map(y => y.toByte)).toArray)
    data
  }

  private def sendSampleData(data: Array[Byte]): Unit = {
    val request = SamplingRequest(id = this.id, data = ByteString.copyFrom(data))
    val response = stub.sampling(request)

    response.onComplete {
      case Success(value) =>
        if (value.isChecked) {
          this.idKeyRange = mutable.Map.from(value.idKeyRange.map(x => (x._1, new KeyRange(x._2.lowerBound, x._2.upperBound))))
          sortingData()
        } else {
          Worker.logger.warn("Partitioning Failed.")
        }
      case Failure(exception) => Worker.logger.warn(s"RPC failed: $exception")
    }
  }

  // Sorting Data with Keys
  private def sortingData(): Unit = {
    val data = inputDirectories.map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .map(Source.fromFile(_))
      .toList

    for (d <- data) {
      val result = d.grouped(100).toList.map(line => {
        val (key, value) = line.splitAt(10)
        (key.map(_.toByte).toArray, value.map(_.toByte).toArray)
      }).sortBy(_._1)(KeyOrdering)
      saveSortedData(result)
    }
  }

  // Save Sorted Data
  private def saveSortedData(result: List[(Array[Byte], Array[Byte])]): Unit = {
    val iter = idKeyRange.iterator
    var (id, keyRange) = iter.next()

    def createNewFile(): FileOutputStream = {
      val fileName = s"$outputDirectory/worker${id}_${Random.alphanumeric.take(10).mkString}"
      new FileOutputStream(new File(fileName))
    }

    var outputStream = createNewFile()

    for (rs <- result) {
      if (KeyOrdering.compare(rs._1, keyRange.upperBound.toByteArray) > 0) {
        outputStream.close()
        val (newId, newKeyRange) = iter.next()
        id = newId
        keyRange = newKeyRange
        outputStream = createNewFile()
      }
      outputStream.write(rs._1)
      outputStream.write(rs._2)
    }
    outputStream.close()
  }

  def startShufflingServer(): Unit={
    this.serverPort = findRandomPort
    workerServer = ServerBuilder.forPort(this.serverPort)
      .addService(ShuffleGrpc.bindService(new ShuffleImpl, ExecutionContext.global))
      .build
      .start()

    logger.info(s"Shuffling Server is started at port ${this.serverPort}")
  }

  def stopShufflingServer(): Unit={
    if (workerServer != null) {
      workerServer.shutdown()
      logger.info("Shuffling Server stopped.")
    }
  }

  def setShufflingServer(): Unit={
    val request = ShufflingServerRequest(id = this.id, port = this.serverPort)
    val response = stub.shufflingReady(request)

    response.onComplete {
      case Success(value) => {
        this.idAddress = mutable.Map.from(value.idPort)
        Worker.logger.info(s"Endpoint: ${this.idAddress}")
        shuffle()
        shufflingCompleted()
      }
      case Failure(e) =>
        Worker.logger.error(s"SetShufflingServer failed: $e")
    }
  }

  def shuffle(): Unit={
    for((targetId, targetEndpoint) <- this.idAddress.filter(_._1 != this.id)) {
      logger.info(s"Sending files from ${this.id} to $targetId")
      val filesToSend = getAllFilesOfId(targetId)

      val finishSignal = new CountDownLatch(1)
      var sendSignal = new CountDownLatch(1)

      val splittedEndpoint = targetEndpoint.split(':')
      val shufflingChannel = ManagedChannelBuilder
        .forAddress(splittedEndpoint(0), splittedEndpoint(1).toInt)
        .usePlaintext()
        .build()
      val shufflingStub = ShuffleGrpc.stub(shufflingChannel)
      val responseObserver = shufflingStub.shuffling(new StreamObserver[ShufflingResponse] {
        override def onNext(value: ShufflingResponse): Unit = sendSignal.countDown()

        override def onError(error: Throwable): Unit = {
          logger.error(s"Send files faild: $error")
          finishSignal.countDown()
        }

        override def onCompleted(): Unit = {
          finishSignal.countDown()
        }
      })

      for (file<-filesToSend) {
        val source = Source.fromFile(file)
        val sourceFile = source.toList.map(x=>x.toByte).toArray
        responseObserver.onNext(ShufflingRequest(data = ByteString.copyFrom(sourceFile)))

        sendSignal.await()
        sendSignal = new CountDownLatch(1)
      }

      responseObserver.onCompleted()
      finishSignal.await()
      logger.info(s"Finish sending files from worker id: ${this.id} to worker id: $targetId")
    }
  }

  def saveShuffledFile(file: Array[Byte]): Unit = {
    val filename = s"${this.outputDirectory}/worker_s${this.id}_${Random.alphanumeric.take(10).mkString}"
    logger.info(s"save received file: $filename")
    val outputFile = new File(filename)
    val outputStream = new FileOutputStream(outputFile)
    outputStream.write(file)
    outputStream.close()
  }

  private class ShuffleImpl extends ShuffleGrpc.Shuffle {
    override def shuffling(responseObserver: StreamObserver[ShufflingResponse]): StreamObserver[ShufflingRequest] = {
      val requestObserver = new StreamObserver[ShufflingRequest] {
        override def onNext(value: ShufflingRequest): Unit={
          saveShuffledFile(value.data.toByteArray)
          responseObserver.onNext(ShufflingResponse(isChecked = true))
        }
        override def onError(error: Throwable): Unit = {
          logger.error(s"Response to SendFiles failed: $error")
        }
        override def onCompleted(): Unit = {
          responseObserver.onCompleted()
        }
      }

      requestObserver
    }
  }

  def shufflingCompleted(): Unit = {
    var num = getNumberOfFiles
    val request = ShufflingCompletedRequest(id = this.id, num = num)
    val response = stub.shufflingCompleted(request)

    response.onComplete {
      case Success(r) => startMerging(r)
      case Failure(e) => logger.error(s"ShufflingCompleted failed: $e")
    }
  }

  def getAllFilesOfId(id: Int): List[File]={
    val getFiles = new File(outputDirectory)
    getFiles
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.startsWith(s"$id"))
      .toList
  }

  def getNumberOfFiles: Int = {
    inputDirectories
      .map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .size
  }

  def getTargetedFiles(): List[File] = {
    val getFiles = new File(outputDirectory)
    getFiles
      .listFiles()
      .filter(_.isFile())
      .filter(_.getName.startsWith(s"${this.id}_"))
      .toList
  }
}