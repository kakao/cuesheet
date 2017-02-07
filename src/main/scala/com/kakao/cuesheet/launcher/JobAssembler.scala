package com.kakao.cuesheet.launcher

import java.io.{BufferedOutputStream, File}
import java.net.URL
import java.nio.file.{Files, Path}
import java.util.jar.{JarEntry, JarFile, JarInputStream}
import java.util.zip.{ZipEntry, ZipException, ZipOutputStream}

import com.google.common.io.ByteStreams
import com.google.common.io.Files._
import com.kakao.cuesheet.CueSheetVersion
import com.kakao.cuesheet.deps._
import com.kakao.mango.io.{AutoClosing, FileSystems, JarStreams}
import com.kakao.mango.logging.Logging
import com.kakao.mango.util.Conversions

object JobAssembler extends Conversions with Logging {

  val HADOOP_FILES = Seq("core-site.xml", "hadoop-env.sh", "hdfs-site.xml", "log4j.properties", "mapred-site.xml", "topology.map", "topology.py", "yarn-site.xml", "hive-site.xml")

  /** make application assembly composed of the non-core Jars, and return the path */
  def assembleDependencies(mainClass: String = "", confPath: Path = null, loader: ClassLoader = getClass.getClassLoader): String = {
    val (_, applicationJars) = DependencyAnalyzer(loader).graph.divide()

    findAssembly(applicationJars) match {
      case Some(assembly) =>
        // assembly found among the jars
        assembly.path

      case None =>
        // create a new assembly
        val jars = prioritize(applicationJars)
        logger.info(s"Running assembly of ${jars.size} jars: ${jars.map(jar => new File(jar.path).getName).mkString(",")}")

        val tmpdir = createTempDir().toPath
        val assemblyPath = tmpdir.resolve(s"assembly-$mainClass.jar")
        val assembly = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(assemblyPath)))

        assembly.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"))
        assembly.write("Manifest-Version: 1.0\n".bytes)
        assembly.write("Implementation-Vendor: com.kakao.cuesheet\n".bytes)
        assembly.write("Implementation-Title: cuesheet-assembly\n".bytes)
        assembly.write(s"Implementation-Version: ${CueSheetVersion.version}\n".bytes)
        assembly.write("".bytes)
        if (mainClass.nonEmpty) {
          assembly.write(s"Main-Class: $mainClass\n".bytes)
        }
        assembly.closeEntry()

        if (confPath != null) {
          FileSystems.entries(confPath).foreach { path =>
            val file = path.toFile
            if (file.isFile && file.canRead) {
              logger.debug(s"Copying Hadoop configuration file $file")
              assembly.putNextEntry(new ZipEntry(file.getName))
              Files.copy(path, assembly)
              assembly.closeEntry()
            }
          }
        }

        AutoClosing(assembly) { output =>
          val results = for (jar <- jars) yield {
            copyJarEntries(new URL("file:" + jar.path), output)
          }
          logger.info(s"Created assembly of size ${results.sum / 1000 / 1e3} MB")
        }
        logger.info(s"Compressed size: ${Files.size(assemblyPath) / 1000 / 1e3} MB")

        assemblyPath.toString
    }
  }

  /** find the assembly jar among given jars */
  private def findAssembly(jars: Seq[DependencyNode]): Option[DependencyNode] = {
    jars.find { node =>
      val manifest = new JarFile(node.path).getManifest
      manifest != null && manifest.getMainAttributes.getValue("Implementation-Title") == "cuesheet-assembly"
    }
  }

  /** make unmanaged classes and Kakao jars come first */
  private def prioritize(jars: Seq[DependencyNode]): Seq[DependencyNode] = {
    val (unmanaged, managed) = jars.partition(_.isInstanceOf[UnmanagedDependencyNode])
    val (kakao, others) = jars.partition {
      case node: ManagedDependencyNode => node.group.startsWith("com.kakao")
      case _ => false
    }
    unmanaged ++ kakao ++ others
  }

  /** copy all entries in the jar pointed by given URL to target */
  private def copyJarEntries(source: URL, target: ZipOutputStream): Long = {
    AutoClosing(new JarInputStream(source.openStream())) { input =>
      var total = 0L
      for (entry <- JarStreams.entries(input) if !entry.isDirectory) {
        try {
          target.putNextEntry(new JarEntry(entry.getName))
          total += ByteStreams.copy(input, target)
          target.closeEntry()
        } catch {
          case e: ZipException if e.getMessage.contains("duplicate entry") =>
            logger.trace(s"skipping duplicate entry ${e.getMessage.split(":")(1)}")
          case e: Throwable =>
            logger.error(s"Exception during assembly: ${e.getMessage}")
            throw new RuntimeException("Exception during assembly", e)
        }
      }
      logger.debug(s"copied $total bytes from $source")
      total
    }
  }

}
