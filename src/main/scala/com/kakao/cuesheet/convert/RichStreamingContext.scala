package com.kakao.cuesheet.convert

import java.util.concurrent.ScheduledFuture
import java.util.concurrent.atomic.AtomicReference

import com.kakao.mango.concurrent._
import com.kakao.mango.json._
import com.kakao.mango.logging.Logging
import com.kakao.mango.zk.ZooKeeperConnection
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaClusterHook, KafkaUtils}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

class RichStreamingContext(ssc: StreamingContext) extends Logging {

  def generated[T: ClassTag](generate: () => TraversableOnce[T], rate: Duration, level: StorageLevel = MEMORY_AND_DISK_SER_2): InputDStream[T] = {
    ssc.receiverStream(new Receiver[T](level) {
      var job: ScheduledFuture[_] = null

      override def onStart(): Unit = {
        job = NamedExecutors.scheduled("rich-streaming-context").withFixedRate(rate) {
          store(generate().toIterator)
        }
      }

      override def onStop(): Unit = {
        job.cancel(true)
      }
    })
  }

  def generated[T: ClassTag](rate: Duration, level: StorageLevel)(generate: => T): InputDStream[T] = generated(() => Seq(generate), rate)

  def generated[T: ClassTag](rate: Duration)(generate: => T): InputDStream[T] = generated(rate, MEMORY_AND_DISK_SER_2)(generate)

  /** an empty DStream */
  def emptyStream(level: StorageLevel = MEMORY_AND_DISK_SER_2): DStream[Nothing] = {
    new InputDStream[Nothing](ssc) {
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def compute(validTime: Time): Option[RDD[Nothing]] = None
    }
  }

  /** a DStream that calculates a value every batch interval */
  def schedulingStream(level: StorageLevel = MEMORY_AND_DISK_SER_2): DStream[Time] = {
    new InputDStream[Time](ssc) {
      override def start(): Unit = {}
      override def stop(): Unit = {}
      override def compute(validTime: Time): Option[RDD[Time]] = Some(ssc.sparkContext.parallelize(Seq(validTime)))
    }
  }

  /** schedule a task for every batch interval */
  def foreachBatch(job: Time => Unit): Unit = {
    schedulingStream().foreachRDD((rdd, time) => job(time))
  }

  def resumableKafkaStream(quorum: String, groupId: String, topic: String): InputDStream[String] = {
    resumableKafkaStream[String, String, StringDecoder, StringDecoder, String](
      quorum, groupId, topic, Map(), (mmd: MessageAndMetadata[String, String]) => mmd.message()
    )
  }

  def resumableKafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag] (
    quorum: String, groupId: String, topic: String, additionalKafkaParams: Map[String, String] = Map()): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key(), mmd.message())
    resumableKafkaStream[K, V, KD, VD, (K, V)](quorum, groupId, topic, additionalKafkaParams, messageHandler)
  }

  /** Rather than using checkpoints, this uses the offset information stored in ZooKeeper,
    * so it can be used even if the number of partition changes; however, an application restart is needed.
    */
  def resumableKafkaStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag] (
    quorum: String, groupId: String, topic: String,
    additionalKafkaParams: Map[String, String],
    messageHandler: MessageAndMetadata[K, V] => R): InputDStream[R] = {

    // find the brocker list from ZooKeeper
    val zk = ZooKeeperConnection(quorum)
    val ids = zk("brokers", "ids")
    val brokers = for {
      child <- ids.children().sync().sorted
    } yield {
      val map = parseJson(ids.child(child).string().sync())
      s"${map("host")}:${map("port")}"
    }
    logger.info(s"Found ${brokers.size} brokers from quorum $quorum: ${brokers.mkString(",")}")

    val kafkaParams = additionalKafkaParams ++ Map("metadata.broker.list" -> brokers.mkString(","))

    // read the last offsets
    val offsetNode = zk("consumers", groupId, "offsets", topic)
    val lastOffsets = offsetNode.childrenOrEmpty().sync().map { p =>
      TopicAndPartition(topic, p.toInt) -> offsetNode.child(p).string().sync().toLong
    }.toMap
    logger.info(s"last offsets read from ZooKeeper: ${lastOffsets.toSeq.sortBy(_._1.partition)}")

    // read the partition list and latest offsets
    val kc = KafkaClusterHook.newCluster(kafkaParams)
    val leaderOffsets = (for {
      partitions <- kc.getPartitions(Set(topic)).right
      offsets <- kc.getLatestLeaderOffsets(partitions).right
    } yield {
      offsets.map { case (tp, lo) => (tp, lo.offset) }
    }).fold(err => throw new SparkException(err.mkString("\n")), ok => ok)

    val fromOffsets = if (lastOffsets.isEmpty) {
      logger.info(s"No offsets found in ZooKeeper; Starting from latest offsets: ${leaderOffsets.toSeq.sortBy(_._1.partition)}")
      leaderOffsets
    } else {
      val offsets = leaderOffsets.map { case (tp, offset) =>
        lastOffsets.get(tp) match {
          case Some(last) => (tp, last) // use the offsets saved in ZooKeeper
          case None => (tp, 0L)         // use offset 0 if additional partitions are found
        }
      }
      logger.info(s"Starting from offsets specified in ZooKeeper: ${offsets.toSeq.sortBy(_._1.partition)}")
      offsets
    }

    // save the current offset
    val offsetHolder = new AtomicReference[Seq[(Int, Long)]]()

    // update offset every batch using StreamingListener
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(event: StreamingListenerBatchCompleted): Unit = {
        val info = event.batchInfo
        logger.info(s"Batch completed: $event")

        Option(offsetHolder.getAndSet(null)) match {
          case Some(offsets) =>
            Future.sequence(offsets.map { case (partition, offset) =>
              zk("consumers", groupId, "offsets", topic, partition.toString).setOrCreate(offset.toString)
            }).onComplete {
              case Success(_) => logger.info(s"Committed offsets : $offsets")
              case Failure(e) => logger.error(s"Exception occurred while committing offsets", e)
            }
          case None =>
            logger.warn(s"No offset information was available for batch: $info")
        }
      }
    })

    logger.info(s"kafkaParams: $kafkaParams")
    logger.info(s"fromOffsets: $fromOffsets")

    val stream = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, fromOffsets, messageHandler)

    stream.foreachRDD { (rdd, time) =>
      // save the current offsets
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.map {
        range => range.partition -> range.untilOffset
      }.toSeq.sortBy(_._1)
      offsetHolder.set(offsets)
      logger.info(s"saved current offsets: $offsets")
    }

    stream
  }

}
