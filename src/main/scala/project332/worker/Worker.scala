package project332.worker

import java.util.concurrent.TimeUnit
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage}
import project332.example.{SamplingGrpc, SampleSendRequest, SampleSendReply}
import com.typesafe.scalalogging.LazyLogging
import java.io.File
import scala.io.Source
import scala.util.{Success, Failure}
import org.apache.loggin.log4j.scala.Logging

object Worker {
  def apply(host: String, port: Int): Worker = {
    val channel = ManagedChannelBuilder.forAddress(host, port)
      .usePlaintext()
      .build()
    val blockingStub = ExampleServiceGrpc.blockingStub(channel)
    new Worker(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = Worker("127.0.0.1", 50051)
    try {
      val user = args.headOption.getOrElse("world")
      client.greet(user)
    } finally {
      client.shutdown()
    }
  }
}

class Worker private(
                      private val channel: ManagedChannel,
                      private val blockingStub: ExampleServiceGrpc.ExampleServiceBlockingStub
                    ) extends LazyLogging {

  // shutdown 메서드: gRPC 채널 종료
  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  // greet 메서드: 서버로 요청을 보내고 응답을 받아오는 메서드
  def greet(name: String): Unit = {
    logger.info(s"Will try to greet $name ...")
    val request = RequestMessage(name = name)
    try {
      val response = blockingStub.sayHello(request)
      logger.info(s"Greeting: ${response.message}")
    } catch {
      case e: StatusRuntimeException =>
        logger.warn("RPC failed: " + e.getStatus)
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
}