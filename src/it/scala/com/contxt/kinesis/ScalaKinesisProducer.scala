/*
 *  Taken from https://github.com/StreetContxt/kpl-scala/tree/master/src/main/scala/com/contxt/kinesis
 */
package com.contxt.kinesis

import com.amazonaws.services.kinesis.producer.{ KinesisProducer, KinesisProducerConfiguration, UserRecordResult }
import com.google.common.util.concurrent.ListenableFuture
import com.typesafe.config.{ Config, ConfigFactory }
import java.nio.ByteBuffer
import scala.concurrent._
import scala.language.implicitConversions
import scala.util.Try
import collection.JavaConverters._

import scala.concurrent.ExecutionContext.Implicits.global
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import scala.util.control.NonFatal


/** A lightweight Scala wrapper around Kinesis Producer Library (KPL). */
trait ScalaKinesisProducer {

  def streamId: StreamId

  /** Sends a record to a stream. See
    * [[[com.amazonaws.services.kinesis.producer.KinesisProducer.addUserRecord(String, String, String, ByteBuffer):ListenableFuture[UserRecordResult]*]]].
    */
  def send(partitionKey: String, data: ByteBuffer, explicitHashKey: Option[String] = None): Future[UserRecordResult]

  /** Performs an orderly shutdown, waiting for all the outgoing messages before destroying the underlying producer. */
  def shutdown(): Future[Unit]
}

object ScalaKinesisProducer {
  def apply(
    streamName: String,
    kplConfig: KinesisProducerConfiguration,
    config: Config = ConfigFactory.load()
  ): ScalaKinesisProducer = {
    val producerStats = ProducerStats.getInstance(config)
    ScalaKinesisProducer(streamName, kplConfig, producerStats)
  }

  def apply(
    streamName: String,
    kplConfig: KinesisProducerConfiguration,
    producerStats: ProducerStats
  ): ScalaKinesisProducer = {
    val streamId = StreamId(kplConfig.getRegion, streamName)
    val producer = new KinesisProducer(kplConfig)
    new ScalaKinesisProducerImpl(streamId, producer, producerStats)
  }

  private[kinesis] implicit def listenableToScalaFuture[A](listenable: ListenableFuture[A]): Future[A] = {
    val promise = Promise[A]
    val callback = new Runnable {
      override def run(): Unit = promise.tryComplete(Try(listenable.get()))
    }
    listenable.addListener(callback, ExecutionContext.global)
    promise.future
  }
}

private[kinesis] class ScalaKinesisProducerImpl(
  val streamId: StreamId,
  private val producer: KinesisProducer,
  private val stats: ProducerStats
) extends ScalaKinesisProducer {
  import ScalaKinesisProducer.listenableToScalaFuture

  stats.reportInitialization(streamId)

  def send(partitionKey: String, data: ByteBuffer, explicitHashKey: Option[String]): Future[UserRecordResult] = {
    stats.trackSend(streamId, data.remaining) {
      producer.addUserRecord(streamId.streamName, partitionKey, explicitHashKey.orNull, data).map { result =>
        if (!result.isSuccessful) throwSendFailedException(result) else result
      }
    }
  }

  def shutdown(): Future[Unit] = shutdownOnce

  private lazy val shutdownOnce: Future[Unit] = {
    val allFlushedFuture = flushAll()
    val shutdownPromise = Promise[Unit]
    allFlushedFuture.onComplete { _ =>
      shutdownPromise.completeWith(destroyProducer())
    }
    val combinedFuture = allFlushedFuture.zip(shutdownPromise.future).map(_ => ())
    combinedFuture.onComplete(_ => stats.reportShutdown(streamId))
    combinedFuture
  }

  private def throwSendFailedException(result: UserRecordResult): Nothing = {
    val attemptCount = result.getAttempts.size
    val errorMessage = result.getAttempts.asScala.lastOption.map(_.getErrorMessage)
    throw new RuntimeException(
      s"Sending a record to $streamId failed after $attemptCount attempts, last error message: $errorMessage."
    )
  }

  private def flushAll(): Future[Unit] = {
    Future {
      blocking {
        producer.flushSync()
      }
    }
  }

  private def destroyProducer(): Future[Unit] = {
    Future {
      blocking {
        producer.destroy()
      }
    }
  }
}

trait ProducerStats {
  def trackSend(streamId: StreamId, size: Int)(closure: => Future[UserRecordResult]): Future[UserRecordResult]
  def reportInitialization(streamId: StreamId): Unit
  def reportShutdown(streamId: StreamId): Unit
}

object ProducerStats {
  private val log = LoggerFactory.getLogger(classOf[ProducerStats])

  def getInstance(config: Config): ProducerStats = {
    try {
      val className = config.getString("com.contxt.kinesis.producer.stats-class-name")
      Class.forName(className).newInstance().asInstanceOf[ProducerStats]
    }
    catch {
      case NonFatal(e) =>
        log.error("Could not load a `ProducerStats` instance, falling back to `NoopProducerStats`.", e)
        new NoopProducerStats
    }
  }
}

class NoopProducerStats extends ProducerStats {
  def trackSend(streamId: StreamId, size: Int)(closure: => Future[UserRecordResult]): Future[UserRecordResult] = closure
  def reportInitialization(streamId: StreamId): Unit = {}
  def reportShutdown(streamId: StreamId): Unit = {}
}

case class StreamId(
  /** AWS region name. */
  regionName: String,

    /** Stream name. */
  streamName: String
)
