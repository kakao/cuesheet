package org.apache.spark.deploy.yarn

import com.kakao.mango.logging.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.{ApplicationId, LocalResource}
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** A wrapper arround Spark's YARN Client to be able to obtain the YARN application ID */
class CueSheetYarnClient(args: ClientArguments, hadoopConf: Configuration, sparkConf: SparkConf) extends Client(args, hadoopConf, sparkConf) with Logging {
  val listeners = new ArrayBuffer[ApplicationId => Unit]
  def addListener(listener: ApplicationId => Unit): Unit = listeners += listener

  override def prepareLocalResources(destDir: Path, pySparkArchives: Seq[String]): mutable.HashMap[String, LocalResource] = {
    // Prevent UserGroupInformation.isSecurityEnabled unexpectedly changing to `false`.
    UserGroupInformation.setConfiguration(hadoopConf)
    super.prepareLocalResources(destDir, pySparkArchives)
  }

  override def submitApplication(): ApplicationId = {
    val appId = super.submitApplication()
    listeners.foreach(_(appId))
    appId
  }
}

/** Exposes Spark's YARN Client implementation */
object CueSheetYarnClient {
  def run(hadoopConf: Configuration, sparkConf: SparkConf, argStrings: Array[String], appIdCallback: String => Unit): Unit = {
    System.setProperty("SPARK_YARN_MODE", "true")
    // SparkSubmit would use yarn cache to distribute files & jars in yarn mode,
    // so remove them from sparkConf here for yarn mode.
    sparkConf.remove("spark.jars")
    sparkConf.remove("spark.files")
    val args = new ClientArguments(argStrings)
    val client = new CueSheetYarnClient(args, hadoopConf, sparkConf)
    client.addListener(id => appIdCallback(id.toString))
    client.run()
  }
}
