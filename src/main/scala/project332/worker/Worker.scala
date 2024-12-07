package project332.worker

import java.io.File
import java.util.concurrent.TimeUnit
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import scala.io.Source
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import project332.common.Common.getLocalIP
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage}
import project332.example.{SamplingGrpc, SampleSendRequest, SampleSendReply}
// sampling grpcs
import project332.example.{SamplingGrpc, SampleSendRequest, SampleSendReply}
import project332.example.SamplingGrpc.SamplingStub

object Worker {
  def apply(host: String, port: Int): Worker = {
    val channel = ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build()
    val blockingStub = ExampleServiceGrpc.blockingStub(channel)
    val samplingStub = SamplingGrpc.stub(channel)
    new Worker(channel, blockingStub, samplingStub)
  }

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

  
class Worker private(
                      private val channel: ManagedChannel,
                      private val blockingStub: ExampleServiceGrpc.ExampleServiceBlockingStub,
                      private val samplingStub: SamplingStub,
                      private val masterIp: String,
                      private val masterPort: Int
                    ) extends LazyLogging {
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

  def makeSample: Array[Byte] = {
    val files = inputDirectories.map(new File(_)).flatMap(_.listFiles.filter(_.isFile))
    val fileSources = files.map(Source.fromFile(_))
    val groupedData = fileSources.flatMap(_.grouped(100)).take(10000)
    val keys = groupedData.map(chunk => chunk.dropRight(90))
    logger.info("")
    keys.flatten.map(_.toByte).toArray
  }

  def sendSample(data: Array[Byte]) : Unit = {
    val request = SampleSendRequest(id = this.id, data = ByteString.copyFrom(data))
    logger.info("")
    val response = stub.sendSample(request)
    response.onComplete {
      case Success(value) => {
        handleSendSampleResponse(value)
        
      }
      case Failure(exception) => logger.error("")
    }
  }

  // make sample from files
  def makeSample: Array[Byte] = {
    val files = inputDirectories.map(new File(_)).flatMap(_.listFiles.filter(_.isFile))
    val fileSources = files.map(Source.fromFile(_))
    val groupedData = fileSources.flatMap(_.grouped(100)).take(10000)
    val keys = groupedData.map(chunk => chunk.dropRight(90))
    logger.info("")
    keys.flatten.map(_.toByte).toArray
  }

  // worker send sample to master
  def sendSample(data: Array[Byte]) : Unit = {
    val request = SampleSendRequest(id = this.id, data = ByteString.copyFrom(data))
    logger.info("we have send data")
    val response = samplingStub.sampleSend(request)
    response.onComplete {
      case Success(value) => {
        handleSampleSendReply(value)
        sortFilesWithKeyRanges()
        startGrpcServer()
        setSlaveServerPort()
      }
      case Failure(exception) => logger.error(s"sendSampledData failed: ${exception}")
    }
  }

  // handle reply from master for sending sample
  def handleSampleSendReply(response: SampleSendReply): Unit = {
    assert(response.ok)
    logger.info(s"Send Sampled Data succeeded. id to key ranges: ${response.idToKeyRanges.map(entry => (entry._1, (entry._2.lowerBound.toByteArray.toList, entry._2.upperBound.toByteArray.toList)))}")
    this.idToKeyRange = response.idToKeyRanges.map(entry => (entry._1, new KeyRange(lowerBound = entry._2.lowerBound.toByteArray, upperBound = entry._2.upperBound.toByteArray)))
  }
}