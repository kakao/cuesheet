package org.apache.spark.deploy.yarn

import java.net.{InetAddress, UnknownHostException}

import com.google.common.base.Objects
import com.kakao.mango.logging.Logging
import com.kakao.mango.reflect.Accessible
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileContext, FileSystem, FileUtil, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.spark.SparkConf
import org.apache.spark.deploy.yarn.Client.APP_FILE_PERMISSION

import scala.collection.mutable.ArrayBuffer

/** A wrapper arround Spark's YARN Client to be able to obtain the YARN application ID */
class CueSheetYarnClient(args: ClientArguments, hadoopConf: Configuration, sparkConf: SparkConf) extends Client(args, hadoopConf, sparkConf) with Logging {
  val listeners = new ArrayBuffer[ApplicationId => Unit]
  def addListener(listener: ApplicationId => Unit): Unit = listeners += listener

  override def prepareLocalResources(destDir: Path, pySparkArchives: Seq[String]) = {
    // No idea why UserGroupInformation.isSecurityEnabled changes to `false`.
    UserGroupInformation.setConfiguration(hadoopConf)
    super.prepareLocalResources(destDir, pySparkArchives)
  }

  override def submitApplication(): ApplicationId = {
    val appId = super.submitApplication()
    listeners.foreach(_(appId))
    appId
  }
//
//  // the method in object Client is not accessible from here;
//  // putting a duplicate method here for stability, rather than using reflection
//  private[yarn] def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
//    val (srcUri, dstUri) = (srcFs.getUri, destFs.getUri)
//    if (srcUri.getScheme == null || srcUri.getScheme != dstUri.getScheme) false else {
//      var srcHost = srcUri.getHost
//      var dstHost = dstUri.getHost
//
//      if (srcHost != null && dstHost != null && srcHost != dstHost) {
//        try {
//          srcHost = InetAddress.getByName(srcHost).getCanonicalHostName
//          dstHost = InetAddress.getByName(dstHost).getCanonicalHostName
//        } catch {
//          case e: UnknownHostException => return false
//        }
//      }
//
//      Objects.equal(srcHost, dstHost) && srcUri.getPort == dstUri.getPort
//    }
//  }
//
//
//  /** skip symlink resolution, since it adds hundreds of RTT while CueSheet does not use symlinks here */
//  private[yarn] override def copyFileToRemote(
//    destDir: Path,
//    srcPath: Path,
//    replication: Short,
//    force: Boolean = false,
//    destName: Option[String] = None): Path = {
//    val destFs = destDir.getFileSystem(hadoopConf)
//    val srcFs = srcPath.getFileSystem(hadoopConf)
//    var destPath = srcPath
//    if (force || !compareFs(srcFs, destFs)) {
//      destPath = new Path(destDir, destName.getOrElse(srcPath.getName))
//      logInfo(s"Uploading resource $srcPath -> $destPath")
//      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
//      destFs.setReplication(destPath, replication)
//      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
//    } else {
//      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
//    }
//    // skip symlink resolution
//    destFs.makeQualified(destPath)
//  }

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
