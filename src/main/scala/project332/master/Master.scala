package project332.master

import com.typesafe.scalalogging.LazyLogging
import java.util.concurrent.CountDownLatch
import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}

import project332.common.Common.{findRandomPort, getLocalIP}
import project332.connection.{CommunicateGrpc, ConnectionRequest, ConnectionResponse}
import project332.connection.{SamplingRequest, SamplingResponse}

object Master extends LazyLogging {

  private val randomPort: Int = findRandomPort

  def main(args: Array[String]): Unit = {
    if (args.headOption.isEmpty) return

    val port = args.find(arg => arg == "DEBUG").map(_ => 50052).getOrElse(randomPort)
    val server = new Master(ExecutionContext.global, args.headOption.get.toInt, port)
    server.start()
    server.blockUntilShutdown()
  }
}

class Master(executionContext: ExecutionContext, val numClient: Int, val port: Int) extends LazyLogging {
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  private var workers: Vector[WorkerClient] = Vector.empty
  var data: List[Array[Byte]] = Nil
  var count: Int = 0

  // 서버 시작
  private def start(): Unit = {
    server = ServerBuilder.forPort(this.port)
      .addService(CommunicateGrpc.bindService(new CommunicateImpl, executionContext))
      .build()
      .start()

    Master.logger.info(s"Client Number : $numClient")
    Master.logger.info(s"Server started at $getLocalIP:${this.port}")

    sys.addShutdownHook {
      stop()
      Master.logger.warn("Server shut down")
    }
  }

  // 서버 종료
  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  // 서버 종료 대기
  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def addData(data: String): Unit = {
    this.synchronized {
      count += 1
      this.data :+ data.getBytes().grouped(10).toList
      if (count == numClient) {
        Master.logger.info(s"Received All Data from $count clients.")
        calculatePivot()
      }
    }
  }

  // function that calculate pivot using samples
  private def calculatePivot(): Unit = {
    val mindata: Array[Byte] = Array(Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue) // 바이트 타입의 최솟값
    val maxdata: Array[Byte] = Array(Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue)
    val sortedSample = this.data.sorted(KeyOrdering)

    val partition: Map[Int,(Array[Byte], Array[Byte])] = Map.empty
    if (this.workers.length == 1) { // slave 하나밖에 없으면 그냥 범위에 냅다 최소~최대 전부 할당  
      partition.put(workers(0).id, (mindata, maxdata))
    } else {
      val range: Int = sortedSample.length / this.workers.length
      val remainder: Int = sortedSample.length % this.workers.length // 나머지 계산

      var loop = 0
      for (worker <- this.workers.toList) {
        // 각 워커가 처리할 데이터의 범위 계산
        val startIdx = loop * range + Math.min(loop, remainder) // 시작 인덱스
        val endIdx = startIdx + range + (if (loop < remainder) 1 else 0) // 끝 인덱스

        // 데이터의 시작과 끝 인덱스를 사용하여 슬레이브에 할당
        if (loop == 0) {
          val bytes = sortedSample(endIdx).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(worker.id, (mindata, bytes))
        } else if (loop == this.slaves.length - 1) {
          partition.put(worker.id, (sortedSample(startIdx), maxdata))
        } else {
          val bytes = sortedSample(endIdx).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(worker.id, (sortedSample(startIdx), bytes))
        }
        loop += 1
      }
    }
    this.idToKeyRanges = partition.map(x=>(x._1, KeyRanges(lowerBound = ByteString.copyFrom(x._2._1), upperBound = ByteString.copyFrom(x._2._2))))
    this.sampledKeyData = Nil
  }

  private def addWorker(ipAddress: String): Unit = {
    this.synchronized {
      this.workers = this.workers :+ new WorkerClient(this.workers.length, ipAddress)
      if (this.workers.length == this.numClient)
        Master.logger.info(s"${this.workers.mkString(", ")}")
    }
  }

  // gRPC 서비스 구현
  private class CommunicateImpl extends CommunicateGrpc.Communicate {
    override def connecting(req: ConnectionRequest): Future[ConnectionResponse] = {
      Master.logger.info(s"Handshake from ${req.ipAddress}")
      clientLatch.countDown()
      addWorker(req.ipAddress)
      clientLatch.await()

      val reply = ConnectionResponse(isConnected = true, id = clientLatch.getCount.toInt)
      Future.successful(reply)
    }

    override def sampling(req: SamplingRequest): Future[SamplingResponse] = {
      Master.logger.info(s"Data from ${req.ipAddress}")
      clientLatch.countDown()
      addData(req.data)
      clientLatch.await()

      val reply = SamplingResponse(isChecked = true)
      Future.successful(reply)
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

  // function that receive sample from workers
  private def receiveSample(id: Int, sampledData : Array[Byte]): Unit = {
    this.synchronized({
      val slave = this.slaves.find(_.id == id).get
      assert(!slave.gotSampledData)
      slave.gotSampledData = true

      this.sampledKeyData = this.sampledKeyData ++ sampledData.grouped(10).toList
      if (this.slaves.count(_.gotSampledData) == this.numClient) {
        logger.info("we receive all the sampled data")
        calculatePivot()
        transitionToSorting()
        transitionToShuffling()
      }
    })
  }

  // function that calculate pivot using samples
  private def calculatePivot(): Unit = {
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
}

class WorkerClient(val id: Int, val ip: String) {
  override def toString: String = ip
}