package project332.worker

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import scala.io.Source
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import project332.common.Common.getLocalIP
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse, SamplingRequest}

object Worker extends LazyLogging {
  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty || args.length != 1) {
      println("Usage: ./worker <Master IP>:<Master Port>")
      System.exit(1)
    }

    val masterIP = args.headOption.get.split(':')
    Worker.logger.info(s"Try to connect with Master : ${masterIP(0)}")
    val client = Worker(masterIP(0), masterIP(1).toInt)

    try { client.initialConnect() }
    catch { case e: Exception => client.shutdown() }

    val path = args.lastOption
    if (path.isEmpty) { Worker.logger.info("File Path is Empty."); client.shutdown() }
    else {
      Worker.logger.info("Try to send Data to Master.")
      val fileSource = Source.fromFile(args(0))
      val keyList = fileSource.grouped(100).map(x => x.dropRight(90)).take(10000).toString()

      try { client.sendData(keyList) }
      finally { client.shutdown() }
    }
  }

  def apply(ip: String, port: Int): Worker = {
    val channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
    val blockingStub = CommunicateGrpc.blockingStub(channel)
    new Worker(channel, blockingStub)
  }
}

class Worker(private val channel: ManagedChannel,
             private val blockingStub: CommunicateGrpc.CommunicateBlockingStub,
            ) extends LazyLogging {

  var pivotMapping: Map[Int, KeyRange] = Map.empty

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, SECONDS)
  }

  def initialConnect(): Unit = {
    val request = ConnectionRequest(ipAddress = getLocalIP)
    try {
      val response = blockingStub.connecting(request)
      Worker.logger.info(s"Connection : ${response.isConnected}")
    } catch {
      case e: StatusRuntimeException => Worker.logger.warn(s"Failed : ${e.getStatus}")
    }
  }

  def handleConnectionResponse(response: ConnectionResponse) : Unit = {
    assert(response.isConnected)
    this.id = response.id
    logger.info("Successfully connected. slave id : " + this.id)
    sendSample(makeSample)
  }

  def sendData(data: String): Unit = {
    val request = SamplingRequest(ipAddress = getLocalIP, data = data)
    try {
      val response = blockingStub.sampling(request)
      Worker.logger.info(s"Data Sent : ${response.isChecked}")
    } catch {
      case e: StatusRuntimeException => Worker.logger.warn(s"Failed : ${e.getStatus}")
    }
  }

  // make sample from files
  def makeSample: Array[Byte] = {
    val files = inputDirectories.map(new File(_)).flatMap(_.listFiles.filter(_.isFile))
    val fileSources = files.map(Source.fromFile(_))
    val groupedData = fileSources.flatMap(_.grouped(100)).take(10000)
    val keys = groupedData.map(chunk => chunk.dropRight(90))
    Worker.logger.info("successfully made sample")
    keys.flatten.map(_.toByte).toArray
  }

  // worker send sample to master
  def sendSample(data: Array[Byte]) : Unit = {
    val request = SamplingRequest(id = this.id, data = ByteString.copyFrom(data))
    Worker.logger.info("successfully send sample")
    val response = blockingStub.sampling(request)
    response.onComplete {
      case Success(value) => {
        handleSampleSendReply(value)
        //sortFilesWithKeyRanges()
        //startGrpcServer()
        //setSlaveServerPort()
      }
      case Failure(exception) => logger.error(s"failed to send sample: ${exception}")
    }
  }

  // handle reply from master for sending sample
  def handleSamplingResponse(response: SamplingResponse): Unit = {
    assert(response.isChecked)
    logger.info(s"Send Sampled Data succeeded. id to key ranges: ${response.idToKeyRanges.map(entry => (entry._1, (entry._2.lowerBound.toByteArray.toList, entry._2.upperBound.toByteArray.toList)))}")
    this.pivotMapping = response.partition.map(x => (x._1, new KeyRange(lowerBound = x._2.lowerBound.toByteArray, upperBound = x._2.upperBound.toByteArray)))
  }
}