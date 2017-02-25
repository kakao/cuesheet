package com.kakao.cuesheet

import java.text.SimpleDateFormat
import java.util.Date

import com.kakao.cuesheet.deps.DependencyAnalyzer
import com.kakao.cuesheet.launcher.{JobAssembler, YarnConnector}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.yarn.CueSheetYarnClient
import org.apache.spark.launcher.SparkLauncherHook
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

/** The base type to extend from, to build a CueSheet application.
  * The values of sc, ssc, sqlContext, and spark are accessible in the derived object's body as if
  * it is inside a Spark shell. A typical example would be:
  *
  * {{{
  *   object Example extends CueSheet {
  *     sc.parallelize(1 to 100).sum()
  *   }
  * }}}
  *
  * This class contains the main method, which is the application's entry point,
  * to perform tasks like running in client/cluster mode, or making an assembly for deployment.
  * This class uses Scala's DelayedInit mechanism, so it contains only methods, no variables.
  * The required fields are defined in the superclass.
  */
abstract class CueSheet(additionalSettings: (String, String)*) extends CueSheetBase(additionalSettings: _*) with App {

  import com.kakao.cuesheet.ExecutionConfig.{config, manager, master}

  /** Overrides App.main to implement the entry point, instead of executing the main body.
    * The main class body is still accessible via super.main()
    */
  final override def main(args: Array[String]) {
    init()

    val (sparkConf, hadoopConf) = if (ExecutionConfig.mode == CLIENT || isOnCluster) {
      // launch the main class, if it is configured for client mode or if this JVM is inside cluster already.
      runDriver(args)
    } else {
      // otherwise, deploy this application to the cluster
      runDeploy(args)
    }

    // install the assembly to HDFS, and print the one-liner shell script to launch this application
    if (manager != LOCAL && config.contains("cuesheet.install") && !isOnCluster) {
      installApplication(sparkConf, hadoopConf, config("cuesheet.install"), args)
    }
  }

  /** prints runtime information */
  private def init(): Unit = {
    logger.info(s"Running CueSheet ${CueSheetVersion.version}")

    val fields = getClass.getDeclaredFields.filterNot(_.getName.contains("$"))

    fields.foreach { field =>
      logger.warn(s"""field "${field.getName}" of type ${field.getType.getSimpleName} might not get serialized correctly""")
    }

    if (fields.nonEmpty) {
      logger.warn(
        s"""consider using the double-brace trick like:
           |
           |object ${getClass.getSimpleName.stripSuffix("$")} extends CueSheet {{
           |  // your main logic here ...
           |}}
         """.stripMargin)
    }

    logger.info(s"ClassLoader for spark-hive: " + Class.forName("org.apache.spark.sql.hive.HiveContext").getClassLoader)

    // prints the all jar files, inside the cluster; convenient for debugging
    if (isOnCluster) {
      // This code does not work in Kerberos environment.
      // val jars = DependencyAnalyzer(loader).graph.nodes.map(_.path)
      // logger.info(s"jar files in current classloader $loader:\n${jars.mkString("\n")}")
    }
  }

  /** This method can be overridden to implement something to be executed before the application starts.
    * When loaded from a checkpoint, the CueSheet class body does not get called, but this method does.
    * Possible example is to send a notification about the application launch, including e.g. sc.uiWebUrl, applicationId
    */
  def sparkContextAvailable(sc: SparkContext): Unit = {
    logger.info(s"Spark Context is now available; web UI: ${sc.uiWebUrl.getOrElse("none")}")
    logger.info(s"Application ID: ${sc.applicationId}")
  }

  /** Executes the driver. In client mode, this function is called in the local JVM,
    * and in cluster mode, this function is called inside a remote node,
    * while communicating with this JVM which is running [[runDeploy]].
    */
  private def runDriver(args: Array[String]): (SparkConf, Configuration) = {
    // maybe acquire streaming context, either from the checkpoint or a fresh one.
    val maybeStreaming = sparkConf.getOption("spark.streaming.checkpoint.path") match {
      case Some(checkpoint) =>
        try {
          // calling this lazy val 'ssc', will try to load checkpoint,
          // and throws 'ReadFromCheckpoint' exception, if the loading is succesful.
          val state = ssc.getState()
          // so at this point, a new streaming context has been made.
          logger.info(s"Starting a fresh streaming application. ssc.state=$state")
          //
          sparkContextAvailable(sc)
          super.main(args)
          Some(ssc)
        } catch {
          case ReadFromCheckpoint(streamingContext) =>
            logger.info(s"successfully read checkpoint from $checkpoint")
            sparkContextAvailable(sc)
            Some(streamingContext)
        }
      case None =>
        sparkContextAvailable(sc)
        super.main(args)
        if (streaming) Some(ssc) else None
    }

    maybeStreaming.collect {
      case context =>
        context.start()
        context.awaitTermination()
    }

    if (contextAvailable) {
      sc.stop()
    }

    (sc.getConf, sc.hadoopConfiguration)
  }

  /** deploy the application to a remote cluster */
  private def runDeploy(args: Array[String]): (SparkConf, Configuration) = {
    manager match {
      case YARN =>
        val (hadoopConf, confPath) = YarnConnector.getConfiguration(master)
        val assembly = JobAssembler.assembleDependencies(className, confPath)
        val sparkJars = YarnConnector.getSparkJarsPath(hadoopConf, loader)

        logger.info(s"Launching $name to YARN cluster $master")
        logger.info(s"Spark Jars: $sparkJars")
        logger.info(s"Application assembly: $assembly")

        sparkConf.set("spark.submit.deployMode", "cluster")
        sparkConf.set("spark.yarn.jars", sparkJars)
        sparkConf.set("spark.app.name", name)

        sparkConf.set("spark.hdfs.jars", sparkJars)
        sparkConf.set("spark.cuesheet.assembly", assembly)

        sparkConf.setIfMissing("spark.driver.memory", "512m")
        sparkConf.setIfMissing("spark.executor.memory", "512m")
        sparkConf.setIfMissing("spark.task.cpus", "1")

        if (sparkConf.get("spark.dynamicAllocation.enabled", "false") == "false") {
          sparkConf.setIfMissing("spark.executor.instances", "4")
        }

        val arguments = ArrayBuffer("--jar", assembly, "--class", className)

        arguments ++= args.flatMap { arg => Seq("--arg", arg) }

        logger.info(s"spark-submit arguments: ${arguments.mkString(" ")}")

        CueSheetYarnClient.run(hadoopConf, sparkConf, arguments.toArray, saveApplicationId)

        (sparkConf, hadoopConf)
      case SPARK =>
        throw new NotImplementedError("Spark Standalone mode not implemented yet")
      case MESOS =>
        throw new NotImplementedError("Mesos mode not implemented yet")
      case LOCAL =>
        logger.error("local mode does not support running on cluster")
        throw new RuntimeException("local mode does not support running on cluster")
    }
  }

  private def installApplication(sparkConf: SparkConf, hadoopConf: Configuration, tag: String, args: Array[String]): Unit = {
    val assembly = sparkConf.get("spark.cuesheet.assembly")
    val uploadedAssembly = YarnConnector.uploadAssembly(hadoopConf, assembly, className, tag)
    val jarName = uploadedAssembly.split('/').last
    val sparkJars = sparkConf.get("spark.hdfs.jars")

    val defaultFS = hadoopConf.get("fs.defaultFS")
    val shortJar = uploadedAssembly.stripPrefix(defaultFS)
    val shortSparkJars = sparkJars.stripPrefix(defaultFS)

    val hadoopXML = HadoopUtils.getHadoopXML(hadoopConf)
    val suffix = if (tag.nonEmpty) tag else new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date())
    val dir = s"${jarName.stripSuffix(".jar")}-$tag"
    val arguments = args.map(SparkLauncherHook.quoteForCommandString).mkString(" ")

    println(
      s"""
         |rm -rf $dir && mkdir $dir && cd $dir &&
         |echo $hadoopXML > core-site.xml &&
         |hdfs --config . dfs -get hdfs://$shortJar \\!$jarName &&
         |hdfs --config . dfs -get hdfs://$shortSparkJars &&
         |java -classpath "*" $className $arguments && cd .. && rm -rf $dir
         |
         |""".stripMargin)

    System.out.flush()
  }

}
