package com.kakao.cuesheet
package launcher

import java.io.{BufferedInputStream, File, FilterInputStream, InputStream}
import java.net.URL
import java.nio.file.Files
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.text.SimpleDateFormat
import java.util.Date
import java.util.zip.ZipInputStream

import com.google.common.io.Files.createTempDir
import com.kakao.cuesheet.deps.DependencyAnalyzer
import com.kakao.mango.io.{AutoClosing, FileSystems, ZipStreams}
import com.kakao.mango.logging.Logging
import com.kakao.mango.text.Resource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.security.UserGroupInformation

import scala.xml.XML

object YarnConnector extends Logging {
  val YARN_SCHEME = "yarn:"
  val HTTP_SCHEME = "http:"
  val HTTPS_SCHEME = "https:"
  val CLASSPATH_SCHEME = "classpath:"
  val HADOOP_FILES = Seq("core-site.xml", "hadoop-env.sh", "hdfs-site.xml", "log4j.properties", "mapred-site.xml", "topology.map", "topology.py", "yarn-site.xml", "hive-site.xml")

  class NonClosingInputStream(in: InputStream) extends FilterInputStream(in) {
    override def close(): Unit = { /* ignore close() call */ }
  }

  /** Parses YARN connection information in one of the following format:
    * - yarn:http://cmf/yarn/configuration.zip
    * - yarn:classpath:com/kakao/cuesheet/launcher/demo/
    *
    * which will have appropriate XML configuration files.
    *
    * @param uri    the URI pointing to YARN/Hive configuration, as described above
    * @return       a tuple containing the Hadoop configuration and
    *               the path to a temporary directory that contains all configuration files
    */
  def getConfiguration(uri: String): (Configuration, java.nio.file.Path) = {
    if (!uri.startsWith(YARN_SCHEME)) {
      throw new IllegalArgumentException("URI does not start with 'yarn:'")
    }

    val address = uri.substring(YARN_SCHEME.length)

    // a temporary directory to store the configuration files
    val base = createTempDir().toPath

    if (address.startsWith(HTTP_SCHEME) || address.startsWith(HTTPS_SCHEME)) {
      val url = new URL(address)
      logger.info(s"Downloading and extracting hadoop configuration from $address ...")

      AutoClosing.apply(new ZipInputStream(new BufferedInputStream(url.openStream()))) { input =>
        for {
          entry <- ZipStreams.entries(input) if !entry.isDirectory
          file <- Option(entry.getName.split("/").last)
        } {
          if (file.endsWith(".xml")) {
            copyExcludingTopologyScript(new NonClosingInputStream(input), base.resolve(file))
          } else {
            Files.copy(input, base.resolve(file), REPLACE_EXISTING)
          }
        }
      }
    } else if (address.startsWith(CLASSPATH_SCHEME)) {
      val classpath = address.substring(CLASSPATH_SCHEME.length).replace('.', '/')
      logger.info(s"Adding configuration files from classpath: $classpath")

      for {
        file <- HADOOP_FILES
        stream <- Option(Resource.stream(s"$classpath/$file"))
      } yield {
        if (file.endsWith(".xml")) {
          copyExcludingTopologyScript(stream, base.resolve(file))
        } else {
          AutoClosing(stream) { input =>
            Files.copy(input, base.resolve(file), REPLACE_EXISTING)
          }
        }
      }
    } else {
      throw new IllegalArgumentException("URI should have either 'http:', 'https:', or 'classpath:' scheme")
    }

    if (FileSystems.entries(base).isEmpty) {
      throw new RuntimeException(s"No configuration files were found in $address!")
    }

    val conf = new Configuration(true)
    FileSystems.entries(base).foreach { entry =>
      val path = entry.toString
      if (path.endsWith(".xml")) {
        conf.addResource(new Path(path))
      }
    }

    // this runs nonexistent python scripts, causing error messages to flood the console
    conf.unset("net.topology.script.file.name")

    UserGroupInformation.setConfiguration(conf)

    (conf, base)
  }

  /** exclude topology script from the XML read from input, and save to the output path */
  private[cuesheet] def copyExcludingTopologyScript(input: InputStream, output: java.nio.file.Path): Unit = {
    val properties = (XML.load(input) \\ "property").filterNot {
      property => (property \ "name").exists(_.text == "net.topology.script.file.name")
    }
    XML.save(output.toString, <configuration>{properties}</configuration>, "UTF-8")
  }

  /** Returns the directory where the Spark Jar files are located at,
    * optionally installing them if the files are not yet installed
    *
    * @param conf     Hadoop configuration to use
    * @param loader   the class loader to analyze Spark dependencies
    * @return         a Hadoop path to the directory, which should be the spark.yarn.jars property
    */
  def getSparkJarsPath(conf: Configuration, loader: ClassLoader = getClass.getClassLoader): String = {
    val graph = DependencyAnalyzer(loader).graph

    val (coreJars, _) = graph.divide()
    val sparkVersion = coreJars.find(d => d.group == "org.apache.spark" && d.artifact.startsWith("spark-core_2.")).get.version

    val fs = FileSystem.get(conf)
    val base = new Path(fs.getHomeDirectory, s".cuesheet/lib/${CueSheetVersion.version}-scala-$scalaVersion-spark-$sparkVersion/")
    fs.mkdirs(base)
    val fileStats = new Iterator[LocatedFileStatus] {
      val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(base, false)
      override def hasNext: Boolean = files.hasNext
      override def next(): LocatedFileStatus = files.next()
    }.collect {
      case status if status.isFile => (status.getPath, status.getLen)
    }.toMap

    var copied = false

    coreJars.foreach { jar =>
      val file = new File(jar.path)
      val name = file.getName
      val target = new Path(base, name)

      // if the existing jar is truncated or has a different length, delete it
      if (fileStats.contains(target) && fileStats(target) != file.length()) {
        fs.delete(target, false)
      }

      // upload the jar if it is not there, truncated, or has a different length
      if (!fileStats.contains(target) || fileStats(target) != file.length()) {
        logger.debug(s"Uploading Spark library $name to $target")
        FileUtil.copy(file, fs, target, false, conf)
        copied = true
      }
    }

    if (copied) {
      logger.info(s"Uploaded Spark $sparkVersion jars to $base")
    }

    new Path(base, "*.jar").toString
  }

  /** upload the application assembly to HDFS */
  def uploadAssembly(conf: Configuration, file: String, className: String, tag: String = ""): String = {
    val assembly: File = new File(file)
    val fs = FileSystem.get(conf)
    val dir = if (tag.nonEmpty) tag else new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date())
    val simpleName = className.split('.').last
    val target = new Path(fs.getHomeDirectory, s".cuesheet/applications/$className/$dir/${simpleName}_$scalaVersion.jar")
    fs.delete(target, true)
    logger.debug(s"Uploading Application Library to $target")
    FileUtil.copy(assembly, fs, target, false, conf)
    target.toString
  }

}
