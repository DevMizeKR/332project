package project332.master

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage}
import project332.connection.{ConnectionRequest, ConnectionReply, InitialConnectGrpc}

object Master extends LazyLogging {

  private val port = 50051

  def main(args: Array[String]): Unit = {
    Master.logger.info("Server started")
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

    Master.logger.info("Server started, listening on ${Master.port}")

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
}