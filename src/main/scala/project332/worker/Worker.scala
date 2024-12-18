package project332.worker

import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.io.{File, FileOutputStream}
import scala.io.Source
import scala.util.{Failure, Success, Random}
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import project332.common.Common.getLocalIP
import project332.common.KeyOrdering
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse, SamplingRequest, SamplingResponse}
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
      .flatMap(_.grouped(100).toList.map(x => x.dropRight(90)).take(1000).flatMap(x => x.map(y => y.toByte)).toArray)
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
}