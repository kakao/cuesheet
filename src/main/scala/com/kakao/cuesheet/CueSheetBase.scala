package com.kakao.cuesheet

import com.kakao.cuesheet.ExecutionConfig._
import com.kakao.cuesheet.convert.Implicits
import com.kakao.cuesheet.launcher.{JobAssembler, YarnConnector}
import com.kakao.mango.concurrent.ConcurrentConverters
import com.kakao.mango.json.JsonConverters
import com.kakao.mango.logging.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.Try

/** The base class of CueSheet which defines the fields to be used in CueSheet applications
  * Since CueSheet extends App, the fields and other initialization logic are defined here instead.
  * This class also mixes in the implicit definitions for JSON and concurrent converters from Mango.
  **/
abstract class CueSheetBase(additionalSettings: (String, String)*)
  extends Implicits
    with JsonConverters
    with ConcurrentConverters
    with Logging {

  /** the fully qualified class name of the CueSheet application class */
  val className: String = getClass.getName.stripSuffix("$")

  /** shorter name, excluding the packages */
  val simpleName: String = getClass.getSimpleName.stripSuffix("$")

  /** append the user's name to the class name; to make the application's name */
  val name: String = className + "@" + ExecutionConfig.sparkUser

  val sparkConf: SparkConf = new SparkConf().setAppName(config.getOrElse("spark.app.name", name)).setAll(config ++ additionalSettings)

  private[cuesheet] val loader = getClass.getClassLoader

  /** A variable indicating if SparkContext is available */
  private[cuesheet] var contextAvailable: Boolean = false

  /** A variable indicating if this app is using Spark Streaming */
  private[cuesheet] var streaming: Boolean = false

  var stopTab: StopTab = _

  var applicationId: Option[String] = None

  private[cuesheet] def saveApplicationId(id: String): Unit = { applicationId = Some(id) }

  /** create SparkContext according to the configuration */
  implicit lazy val sc: SparkContext = {
    contextAvailable = true
    if (streaming) {
      ssc.sparkContext
    } else {
      val context = if (submitting || isOnCluster) {
        // if this JVM is already on cluster, no need to assemble and submit
        new SparkContext(sparkConf)
      } else {
        // client mode spark context creation
        val (actualMaster, assembly) = if (manager == LOCAL) {
          // no need to make assembly in local mode
          (master, "")
        } else if (manager == YARN) {
          val (hadoopConf, base) = YarnConnector.getConfiguration(master)
          val assembly = JobAssembler.assembleDependencies(className, base)
          val sparkJars = YarnConnector.getSparkJarsPath(hadoopConf)

          // copy hadoop configurations to the spark configuration, with "spark.hadoop" prefix.
          // this will then make SparkContext create its Hadoop configuration accordingly.
          hadoopConf.iterator().foreach { entry =>
            sparkConf.set("spark.hadoop." + entry.getKey, entry.getValue)
          }

          // disable the noisy topology script
          sparkConf.remove("spark.hadoop.net.topology.script.file.name")
          sparkConf.set("spark.yarn.jars", sparkJars)
          sparkConf.set("spark.hdfs.jars", sparkJars)
          sparkConf.set("spark.cuesheet.assembly", assembly)

          ("yarn-client", assembly)
        } else {
          // TODO: non-YARN cluster
          val assembly = JobAssembler.assembleDependencies(className)
          (master, assembly)
        }

        sparkConf.setMaster(actualMaster)
        sparkConf.setJars(Array(assembly))
        sparkConf.setAll(config)

        // Add all spark configuration to the system property;
        // this will make SparkHadoopUtil to load UserGroupInformation properly.
        sparkConf.getAll.foreach {
          case (key, value) => System.setProperty(key, value)
        }

        new SparkContext(sparkConf)
      }
      
      // Add Stop Tab to the UI
      stopTab = new StopTab(context)
      applicationId = Some(context.applicationId)
      context
    }
  }

  /** provides the SparkSession */
  implicit lazy val spark: SparkSession = {
    sc // trigger sc to be initialized
    SparkSession.builder().getOrCreate()
  }

  /** provides the HiveContext */
  implicit lazy val sqlContext: HiveContext = {
    val sparkContext = sc
    val hiveContext = new HiveContext(sparkContext)
    hiveContext
  }

  /** same as sqlContext; with a more appropriate name */
  lazy val hiveContext: HiveContext = sqlContext

  case class ReadFromCheckpoint(ssc: StreamingContext) extends RuntimeException

  /** create a StreamingContext from given setting.
    * Spark Streaming applications HAVE to call this before SparkContext */
  implicit lazy val ssc: StreamingContext = {
    val blockInterval = Milliseconds(sparkConf.getLong("spark.streaming.blockInterval", 200L))
    val batchInterval = Milliseconds(sparkConf.getLong("spark.streaming.batchInterval", blockInterval.milliseconds))

    val ssc = sparkConf.getOption("spark.streaming.checkpoint.path") match {
      case Some(path) =>
        if (contextAvailable) {
          throw new RuntimeException("SparkContext has been already created when reading from checkpoint; call 'ssc' first in your code, and then use 'sc' afterwards.")
        }
        var created = false
        val ssc = StreamingContext.getOrCreate(path, () => {
          created = true
          val appName = config.getOrElse("spark.app.name", name)
          val ssc = new StreamingContext(sparkConf, batchInterval)
          ssc.checkpoint(path)
          ssc
        }, createOnError = Try(conf.getBoolean("spark.streaming.checkpoint.createOnError")).getOrElse(false))

        // add stop tab to the UI
        stopTab = new StopTab(ssc.sparkContext)
        stopTab.ssc = ssc

        if (!created) {
          // Since we have loaded all task TAG and logic from the checkpoint; skip running the main function.
          throw ReadFromCheckpoint(ssc)
        }

        ssc
      case None =>
        new StreamingContext(sc, batchInterval)
    }

    streaming = true

    ssc
  }

}
