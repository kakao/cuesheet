package com.kakao.cuesheet.deps

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.net.{URL, URLDecoder}
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.kakao.mango.io.FileSystems
import com.kakao.mango.logging.Logging
import com.kakao.shaded.guava.io.Files.createTempDir

sealed trait DependencyNode {
  def path: String
}

case class ManagedDependency(group: String, artifact: String, classifier: String = "jar")

case class ManagedDependencyNode(
  path: String,
  group: String,
  artifact: String,
  classifier: String,
  version: String,
  children: Seq[ManagedDependency]
) extends DependencyNode {
  def key = ManagedDependency(group, artifact, classifier)
}

case class DirectoryDependencyNode(path: String) extends DependencyNode with Logging {
  lazy val compressed: UnmanagedDependencyNode = {
    val tmpdir = createTempDir()
    val jar = new File(s"${tmpdir.getAbsolutePath}/local-${tmpdir.getName}.jar")
    val root = Paths.get(path)

    val output = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(jar)))
    var count = 0

    FileSystems.entries(root).foreach { path =>
      if (resourceExtensions.exists(path.toString.endsWith)) {
        val entry = new ZipEntry(root.relativize(path).toString)
        output.putNextEntry(entry)
        try {
          Files.copy(path, output)
          count += 1
        } catch {
          case e: IOException => logger.warn(s"skipping $path due to an IOException: ${e.getMessage}")
        }
        output.closeEntry()
      }
    }

    output.close()

    logger.debug(s"Successfully zipped $count files in $path into $jar")

    UnmanagedDependencyNode(jar.getAbsolutePath)
  }
}

case class JavaRuntimeDependencyNode(path: String) extends DependencyNode
case class UnmanagedDependencyNode(path: String) extends DependencyNode

object DependencyNode {

  val resolver = new ChainedArtifactResolver(
    new IvyPathArtifactResolver,
    new IvyOriginalPathArtifactResolver,
    new MavenPathArtifactResolver,
    new GradlePathArtifactResolver,
    new JavaRuntimeResolver,
    new MavenMetadataArtifactResolver,
    new UnmanagedJarResolver
  )

  def resolve(url: URL): DependencyNode = {
    if (url.getProtocol != "file") {
      throw new IllegalArgumentException("non-file dependency is not supported")
    }

    val path = URLDecoder.decode(url.getFile, "UTF-8")
    val file = new File(path)
    if (file.isDirectory) {
      return DirectoryDependencyNode(file.getAbsolutePath)
    }

    if (!file.isFile || !file.canRead) {
      throw new IllegalArgumentException(s"$path is not a file or readable")
    }

    DependencyNode.resolver.resolve(file.getAbsolutePath) match {
      case Some(node) => node
      case None => throw new IllegalArgumentException(s"Could not determine the dependency of $path")
    }
  }
}

