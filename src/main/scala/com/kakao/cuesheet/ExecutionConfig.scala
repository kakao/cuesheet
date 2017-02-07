package com.kakao.cuesheet

import java.lang.management.ManagementFactory

import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.util.Try

object ExecutionConfig {

  // load the Typesafe config, which contains all information about the spark & deployment environment
  // use System properties config.{resource,file,url} to change the configuration file to load
  // default is application.conf in the classpath root
  val conf: Config = ConfigFactory.load()
  val systemUser: String = Option(sys.props("user.name")).getOrElse("unknown")
  val sparkUser: String = Try(conf.getString("spark.user.name")).getOrElse(systemUser)
  val hadoopUser: String = Try(conf.getString("spark.hadoop.user.name")).getOrElse(sparkUser)

  // set hadoop user name
  sys.props("user.name") = hadoopUser
  sys.props("HADOOP_USER_NAME") = hadoopUser

  /** This value is true when someone uses spark-submit, instead of launching a CueSheet-derived class directly */
  val submitting: Boolean = sys.props.contains("SPARK_SUBMIT")

  // Suppress the warning message, pretending we are using spark-submit
  sys.props("SPARK_SUBMIT") = "true"

  /** determine the Typesafe config keys to include in SparkConf */
  def shouldInclude(key: String): Boolean = {
    key.startsWith("spark.")
  }

  /** System properties given as JVM options ('-Dname=value'); CueSheet uses --extra-java-options to apply them to the remove driver */
  val propertyOptions: Seq[(String, String)] = ManagementFactory.getRuntimeMXBean.getInputArguments.toSeq.collect {
    case option if option.startsWith("-D") => option.substring(2)
  }.map { pair =>
    val pos = pair.indexOf('=')
    val (name, value) = if (pos != -1) {
      (pair.substring(0, pos), pair.substring(pos + 1))
    } else {
      (pair, "")
    }

    (name, value)
  }

  /** a Map containing all configs, and the master URL to use */
  lazy val (config, master) = {
    // filter the config entries starting with "spark." in the Typesafe config
    var config = conf.entrySet().collect {
      case entry if shouldInclude(entry.getKey) => (entry.getKey, entry.getValue.unwrapped().toString)
    }.toMap

    // master url; CueSheet allow a custom master scheme for YARN
    val master = config.getOrElse("spark.master", s"local[${Runtime.getRuntime.availableProcessors()}]")

    // remove 'spark.master' from config variable, as we will handle the master url separately
    config -= "spark.master"

    config.foreach {
      case (key, value) => sys.props(key) = value
    }

    // include JVM properties as options
    config ++= propertyOptions

    (config, master)
  }

  /** cluster manager; only YARN and local are fully supported as of now */
  lazy val manager: ClusterManager = master.toLowerCase match {
    case url if url.startsWith("yarn") => YARN
    case url if url.startsWith("spark") => SPARK
    case url if url.startsWith("mesos") => MESOS
    case url if url.startsWith("local") => LOCAL
    case _ =>
      throw new RuntimeException(s"Unknown master scheme: $master")
  }

  /** deploy mode; defaulting to cluster mode in non-local scenario */
  lazy val mode: DeployMode = {
    if (manager == LOCAL)
      CLIENT
    else
      config.get("spark.deploy.mode").map(_.toLowerCase) match {
        case Some("cluster") => CLUSTER
        case Some("client") => CLIENT
        case _ => CLUSTER
      }
  }

}
