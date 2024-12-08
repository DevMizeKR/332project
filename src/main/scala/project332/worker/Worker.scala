package project332.worker

import com.typesafe.scalalogging.LazyLogging
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import scala.io.Source
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise, Await}
import scala.util.{Failure, Success}
import scala.language.postfixOps
import project332.common.Common.getLocalIP
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse, SamplingRequest, SamplingResponse}
import project332.connection.SamplingResponse.KeyRange

import java.io.{File, FileOutputStream, IOException, InputStream}
import scala.concurrent.ExecutionContext.Implicits.global

object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty || args.length != 2) {
      println("Usage: ./worker <Master IP>:<Master Port> <Input Directory>")
      System.exit(1)
    }

    val masterIP = args.headOption.get.split(':')
    val inputDirectories = Array(args.lastOption.get)
    Worker.logger.info(s"Try to connect with Master : ${masterIP(0)}")
    val client = Worker(masterIP(0), masterIP(1).toInt, inputDirectories)
    val done = client.start()
    Await.result(done, Duration.Inf)
    client.shutdown()
  }

  def apply(ip: String, port: Int, inputDirectories: Array[String]): Worker = {
    val channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
    val stub = CommunicateGrpc.stub(channel)
    new Worker(channel, stub, inputDirectories)
  }
}

class Worker(private val channel: ManagedChannel,
             private val stub: CommunicateGrpc.CommunicateStub,
             private val inputDirectories: Array[String]
            ) extends LazyLogging {
  val done: Promise[Boolean] = Promise[Boolean]()
  var pivotMapping: Map[Int, KeyRange] = Map.empty
  var id: Int = 0

  def start(): Future[Boolean] = {
    Future { initialConnect() }
    done.future
  }

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, SECONDS)
  }

  def initialConnect(): Unit = {
    val request = ConnectionRequest(ipAddress = getLocalIP)
    val response = stub.connecting(request)
    response.onComplete {
      case Success(value) =>
        Worker.logger.info(s"Connection : $response")
        handleConnectionResponse(value)
      case Failure(exception) => Worker.logger.warn(s"Failed : $exception")
    }
  }

  def handleConnectionResponse(response: ConnectionResponse) : Unit = {
    assert(response.isConnected)
    this.id = response.id
    logger.info("Successfully connected. slave id : " + this.id)
    sendSample(makeSample)
  }

  // make sample from files
  def makeSample: Array[Byte] = {
    val files = inputDirectories.map(new File(_)).flatMap(_.listFiles.filter(_.isFile))
    val fileSources = files.map(Source.fromFile(_))
    val groupedData = fileSources.flatMap(_.grouped(100)).take(10000)
    val keys = groupedData.map(chunk => chunk.dropRight(90))
    Worker.logger.info("successfully made sample")
    keys.flatten.map(_.toByte)
  }

  // worker send sample to master
  def sendSample(data: Array[Byte]) : Unit = {
    val request = SamplingRequest(id = this.id, data = ByteString.copyFrom(data))
    val response = stub.sampling(request)
    response.onComplete {
      case Success(value) => {
        Worker.logger.info(s"successfully send sample:$data.length")
        handleSamplingResponse(value)
        //sortFilesWithKeyRanges()
        //startGrpcServer()
        //setSlaveServerPort()
      }
      case Failure(exception) => logger.error(s"failed to send sample: $exception")
    }
  }

  // handle reply from master for sending sample
  def handleSamplingResponse(response: SamplingResponse): Unit = {
    assert(response.isChecked)
    logger.info(s"received pivot: ${response.partition.map(entry => (entry._1, (entry._2.lowerbound.toByteArray.toList, entry._2.upperbound.toByteArray.toList)))}")
    this.pivotMapping = response.partition.map(x => (x._1, new KeyRange(lowerbound = x._2.lowerbound, upperbound = x._2.upperbound)))
  }
}