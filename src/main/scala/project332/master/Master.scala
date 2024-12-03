package project332.master

import com.typesafe.scalalogging.LazyLogging
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import project332.example.{ExampleServiceGrpc, RequestMessage, ResponseMessage}
import project332.example.{SamplingGrpc, RequestSampleSend, ResponseSampleSend}

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

  private def createPartition(): Unit = {
    val mindata: Array[Byte] = Array(Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue) // 바이트 타입의 최솟값
    val maxdata: Array[Byte] = Array(Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue)
    val sortedKeyData = this.sampledKeyData.sorted(KeyOrdering)

    val partition: Map[Int,(Array[Byte], Array[Byte])] = Map.empty
    if (this.slaves.length == 1) { // slave 하나밖에 없으면 그냥 범위에 냅다 최소~최대 박음
      partition.put(slaves(0).id, (mindata, maxdata))
    } else {
      val range: Int = sortedKeyData.length / this.slaves.length
      var loop = 0
      for (slave <- this.slaves.toList) {
        if (loop == 0) { // 첫번째 pivot 결정
          val bytes = sortedKeyData((loop + 1) * range).clone() // 밑에서 update로 수정할거니까 copy해서 씀
          bytes.update(9, bytes(9).-(1).toByte) // 10바이트짜리 키의 마지막거에서 1바이트 줄여서 겹치는거 방지
          partition.put(slave.id, (mindata, bytes))
        } else if (loop == this.slaves.length - 1) { // 마지막 pivot 계산
          partition.put(slave.id, (sortedKeyData(loop * range), maxdata))
        } else {
          val bytes = sortedKeyData((loop + 1) * range).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(slave.id, (sortedKeyData(loop * range), bytes))
        } 
        loop +=1
      }
    }
    this.idToKeyRanges = partition.map(x=>(x._1, KeyRanges(lowerBound = ByteString.copyFrom(x._2._1), upperBound = ByteString.copyFrom(x._2._2))))
    // We do not need sampled key data anymore, so it can be garbage collected
    this.sampledKeyData = Nil
  }

  def receiveSample(id: Int, sample: Array[Byte]): Unit = {
    
  }

  private class Sampling extends SamplingGrpc.Sampling {
    override def SampleSend(request: SampleSendRequest) = {
      assert(self.state == Sampling)
      logger.info("")
      receiveSample(request.id, request.data.toByteArray)
      sampleLatch.countDown()
      sampleLatch.await()
      val reply = SampleSendReply(ok = true, idToKeyRanges = self.idToKeyRanges.toMap)
      Future.successful(reply)
    }
  }
}