package project332.worker

import java.util.concurrent.TimeUnit
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage, SortingServiceGrpc, SetShufflingServerRequest, SetShufflingServerPortResponse, ShufflingCompletedRequest, ShufflingCompletedResponse, ShufflingServiceGrpc, SendFileRequest, SendFileResponse}
import com.typesafe.scalalogging.LazyLogging

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
                      private val blockingStub: ExampleServiceGrpc.ExampleServiceBlockingStub,
                      private val inputDirectories: Array[String],
                      private val tempDirectory: String,
                      private val outputDirectory: String,
                    ) extends LazyLogging {

  var serverPort: Int = 0
  var shufflingServer: Server = null
  var idToEndpoint: Map[Int, String] = Map.empty

  /////////////////////shuffling////////////////////////////


  //candidates for Utils
  def findAvailablePort(): Int = {
  }

  def getAllFilesOfId(id: Int): List[File]={
    val getFiles = new File(outputDirectory)
    getFiles
      .listFiles()
      .filter(_.isFile)
      .filter(_.getName.startsWith(s"$id"))
      .toList
  }

  //setting shuffling server
  def startShufflingServer(): Unit={
    this.serverPort = findAvailablePort()

    shufflingServer = ServerBuilder
      .forPort(this.serverPort)
      .addService(shufflingServiceGrpc.bindService(new ShufflingServiceImpl, ExecutionContext.global))
      .build
      .start()
  
    logger.info(s"Shuffling Server is started at port ${this.serverPort}")
  }

  def stopShufflingServer(): Unit={
    if (shufflingServer != null) {
      shufflingServer.shutdown()
      logger.info("Shuffling Server stopped.")
    }
  }

  def handleSetShufflingServerResponse(response: SetShufflingServerResponse): Unit = {
    this.idToEndpoint = response.idToServerEndpoint
    logger.info(s"Endpoint: ${this.idToEndpoint}")
  }

  def setShufflingServer(): Unit={
    val request = SetShufflingServerRequest(id = this.id, port = this.serverPort)
    val response = stub.setShufflingServer(request)

    response.onComplete {
      case Success(r) => {
        handleSetShufflingServerResponse(r)
        shuffle()
        shufflingCompleted()
      }
      case Failure(e) => 
        logger.error(s"SetShufflingServer failed: $e")
    }
  }


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
    
}