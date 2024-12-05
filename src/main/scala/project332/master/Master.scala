package project332.master

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage}

import java.util.concurrent.CountDownLatch
import scala.collection.mutable.Map

object Master extends LazyLogging {

  private val port = 50051

  def main(args: Array[String]): Unit = {
    Master.logger.info(s"Server started")
    val server = new Master(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext) extends LazyLogging {
  private[this] var server: Server = null

  // 서버 시작
  def start(): Unit = {
    server = ServerBuilder
      .forPort(50051) // 포트 지정
      .addService(ExampleServiceGrpc.bindService(new ExampleServiceImpl, ExecutionContext.global))
      .build
      .start()

    Master.logger.info(s"Server started, listening on ${Master.port}")

    sys.addShutdownHook {
      Master.logger.warn("*** shutting down gRPC server since JVM is shutting down")
      stop()
      Master.logger.warn("*** server shut down")
    }
  }

  // 서버 종료
  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
      Master.logger.info("Server stopped.")
    }
  }

  // 서버 종료 대기
  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private val shuffleLatch: CountDownLatch = new CountDownLatch(numClient)
  private val mergeLatch: CountDownLatch = new CountDownLatch(numClient)

  var workers: Vector[WorkerClient] = Vector.empty

  var idToEndpoint: Map[Int, String] = Map.empty

  def setShuffleServerPort(workerId: Int, serverPort: Int): Unit = {
    this.synchronized {
      workers.find(p => p.id == workerId) match {
        case None => logger.error("id does not match!")
        case Some(value) =>
          assert(value.serverPort == 0)
          value.serverPort = serverPort
      }
      if (this.workers.count(_.serverPort != 0) == this.numClient) {
        logger.info("we receive all the worker port")
        setIdToEndpoint()
        //transition to merge phase
      }
    }
  }


  // gRPC 서비스 구현
  private class ExampleServiceImpl extends ExampleServiceGrpc.ExampleService {
    override def sayHello(req: RequestMessage): Future[ResponseMessage] = {
      val name = req.name
      Master.logger.info(s"Received request with name: $name")

      val responseMessage = s"Hello, $name!"
      Master.logger.info(s"Sending response: $responseMessage")

      Future.successful(ResponseMessage(message = responseMessage))
    }
  }

   private class SortingImpl extends SortingGrpc.Sorting {

    override def setShufflingServer(request: SetShufflingServerRequest): Future[SetShufflingServerResponse] = {
      logger.info(s"Set shuffling server port from ${request.id}, server port: ${request.port}")
      setShuffleServerPort(request.id, request.port)
      shuffleLatch.countDown()
      shuffleLatch.await()
      val response = SetShufflingServerPortResponse(ok = true, idToServerEndpoint = self.idToEndpoint.toMap)
      Future.successful(response)
    }

    def getStartIndexAndLength(id: Int): (Int, Int) = {
      //TODO
    }


    override def ShufflingCompleted(req: ShufflingCompletedRequest) = {
      assert(state == SortingStates.Merging)
      setNumFiles(req.id, req.num)
      mergeLatch.countDown()
      mergeLatch.await()
      val (startIndex, length) = getStartIndexAndLength(req.id) //need to be implemented
      val response = ShufflingCompletedResponse(ok = true, startIndex = startIndex, length = length)
      Future.successful(response)
    }

    override def MergingCompleted(req: MergingCompletedRequest) = {
      logger.info(s"worker merge completed. id: ${req.id}")
      setSortingFinished(req.id)
      val response = MergingCompletedResponse(ok = true)
      logger.info(s"worker merge completed response. id: ${req.id}")
      Future.successful(response)
    }

    def setSortingFinished(id: Int) {
      this.synchronized {
        val worker: Option[WorkerClient] = workers.find(x => x.id == id)
        worker match {
          case None => logger.error("id does not match ")
          case Some(value) => value.ended = true
        }
        if (workers.forall(x => x.ended)) {
          server.shutdown()
        }
      }
    }
    }
}