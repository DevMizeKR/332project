package project332.worker

import java.util.concurrent.TimeUnit
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage, SortingServiceGrpc, SetShufflingServerRequest, SetShufflingServerPortResponse, ShufflingCompletedRequest, ShufflingCompletedResponse, ShufflingServiceGrpc, SendFileRequest, SendFileResponse}
import com.typesafe.scalalogging.LazyLogging

import io.grpc.StreamObserver
import java.util.concurrent.CountDownLatch
import scala.io.Source

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

  def getNumberOfFiles: Int = {
    val numberOfFiles = inputDirectories
    inputDirectories
      .map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .size
      //.length
  }

  def getTargetedFiles(): List[File] = {
    val getFiles = new File(outputDirectory)
    getFiles
      .listFiles()
      .filter(_.isFile())
      .filter(_.getName.startsWith(s"${this.id}_"))
      .toList
  }

  /////////////////////shuffling////////////////////////////




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

  /*
  def handleSetShufflingServerResponse(response: SetShufflingServerResponse): Unit = {
    this.idToEndpoint = response.idToServerEndpoint
    logger.info(s"Endpoint: ${this.idToEndpoint}")
  }
  */

  def setShufflingServer(): Unit={
    val request = SetShufflingServerRequest(id = this.id, port = this.serverPort)
    val response = stub.setShufflingServer(request)

    response.onComplete {
      case Success(r) => {
        this.idToEndpoint = response.idToServerEndpoint
        logger.info(s"Endpoint: ${this.idToEndpoint}")
        shuffle()
        shufflingCompleted()
      }
      case Failure(e) => 
        logger.error(s"SetShufflingServer failed: $e")
    }
  }

  def shuffle(): Unit={
    for((targetId, targetEndpoint) <- this.idToEndpoint.filter(_._1 != this.id)) {
      logger.info(s"Sending files from ${this.id} to $targetId")
      val filesToSend = getAllFilesOfId(targetId)

      val finishSignal = new CountDownLatch(1)
      val sendSignal = new CountDownLatch(1)

      val splittedEndpoint = endpiont.split(':')
      val shufflingChannel = ManagedChannelBuilder
        .forAddress(splittedEndpoint(0), splittedEndpoint(1).toInt)
        .usePlaintext()
        .build()
      val shufflingStub = ShufflingServiceGrpc.stub(shufflingChannel)
      val responseObserver = shufflingStub
                              .sendFiles(new StreamObserver[SendFileResponse]{
                                            override def onNext(value: SendFileResponse): Unit = sendSignal.countDown()
                                            override def onErrror(error: Throwable): Unit = {
                                              logger.error(s"Send files faild: $error")
                                              finishSignal.countDown()
                                              }
                                            override def onCompleted(): Unit = finishSignal.countDown()
                                          })

      for (file<-filesToSend) {
        val source = Source.fromFile(file)
        val sourceFile = source.toList.map(x=>x.toByte).toArray
        responseObserver.onNext(SendFileRequest(file=sourceFile))

        //response came
        source.close()
        file.delete()
        sendSignal.await()
        sendSignal = new CountDownLatch(1)
      }

      responseObserver.onCompleted()
      finishSignal.await()
      logger.info(s"Finish sending files from slave id: ${this.id} to slave id: $targetId")
    }
  }

  def saveShuffledFile(file: Array[Byte]): Unit = {
    val filename = s"${this.outputDirectory}/${this.id}_${Random.alphanumeric.take(10).mkString}"
    logger.info(s"save received file: $filename")
    val outputFile = new File(filename)
    val outputStream = new FileOutputStream(outputFile)
    outputStream.write(file)
    outputStream.close()
  }

  private class ShufflingImpl extends ShufflingGrpc.ShufflingService {
    override def SendFiles(responseObserver: StreamObserver[SendFileResponse]): StreamObserver[SendFileRequest] = {
      val requestObserver = new StreamObserver[SendFileRequest] {
        override def onNext(value: SendFileRequest): Unit={
          saveShuffledFile(value.file.toByteArray)
          responseObserver.onNext(SendFileResponse(success = true))
        }
        override def onEffor(error: Throwable): Unit = {
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
    val num = getNumberOfFiles()
    val request = ShufflingCompletedRequest(id = this.id, num = num)
    val response = stub.ShufflingCompleted(request)
    
    response.onComplete {
      case Success(r) => startMerging(r)
      case Failure(e) => logger.error(s"ShufflingCompleted failed: $s")
    }
  }

  ////////////merging////////////////////////

  def startMerging(response: ShufflingCompletedResponse): Unit = {
    assert(response.success)
    val targets = getTargetedFiles()
    //merge(targets, response.startIndex, response.length)
    //mergingCompleted()
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