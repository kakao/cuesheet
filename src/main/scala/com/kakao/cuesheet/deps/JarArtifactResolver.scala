package com.kakao.cuesheet.deps

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths
import java.util.jar.JarFile
import java.util.zip.ZipInputStream

import com.kakao.mango.io.{AutoClosing, FileSystems, ZipStreams}
import com.kakao.mango.logging.Logging

import scala.io.Source
import scala.util.Try
import scala.xml.XML

sealed trait JarArtifactResolver[+T <: DependencyNode] extends Logging {
  /** attempt to resolve the artifact information and its dependencies from given JAR file
    *
    * @param path   the path to the JAR file
    * @return       an Option containing the resolved node or None
    **/
  def resolve(path: String): Option[T]
}

/** Check if the Implementation-Title is "Java Runtime Environment" */
class JavaRuntimeResolver extends JarArtifactResolver[JavaRuntimeDependencyNode] {
  private val base = new File(sys.props("java.home")).getParent

  override def resolve(path: String): Option[JavaRuntimeDependencyNode] = {
    if (path.startsWith(base)) {
      return Some(JavaRuntimeDependencyNode(path))
    }

    val attributes = new JarFile(path).getManifest.getMainAttributes
    if (attributes.getValue("Implementation-Title") == "Java Runtime Environment") {
      return Some(JavaRuntimeDependencyNode(path))
    }

    None
  }
}

/** Resolve dependency from the maven metadata file stored in META-INF/maven.group.id.artifactId/pom.xml
  * This works only for the JARs that were packaged by Maven; SBT is notable exception to this.
  */
class MavenMetadataArtifactResolver extends JarArtifactResolver[ManagedDependencyNode] {
  override def resolve(path: String): Option[ManagedDependencyNode] = {
    AutoClosing(new ZipInputStream(new FileInputStream(path), UTF_8)) { zip =>
      for (entry <- ZipStreams.entries(zip)) {
        val name = entry.getName
        if (name.startsWith("META-INF") && name.endsWith("pom.xml")) {
          val artifact = name.split("/").reverse.apply(1)
          if (path.contains(artifact)) {
            val pom = XML.load(zip)
            return new MavenDependencyEnumerator(pom, path).get
          }
        }
      }
      None
    }
  }
}

/** resolve a dependency in local maven repo, "/.m2/repository/group/id/artifactId/version/artifactId-version.jar" */
class MavenPathArtifactResolver extends JarArtifactResolver[ManagedDependencyNode] {
  override def resolve(path: String): Option[ManagedDependencyNode] = {
    mavenPathFormat.findAllIn(path).matchData.toSeq.headOption.flatMap { m =>
      try {
        val pom = XML.loadFile(path.replaceAll("\\.jar$", ".pom"))
        new MavenDependencyEnumerator(pom, path).get
      } catch {
        case e: Throwable =>
          throw new RuntimeException(s"XML parsing error from $path", e)
      }
    }
  }
}

/** resolve a dependency in local gradle cache */
class GradlePathArtifactResolver extends JarArtifactResolver[ManagedDependencyNode] {

  // TODO: may need to read ivy files as well
  override def resolve(path: String): Option[ManagedDependencyNode] = {
    gradlePathFormat.findAllIn(path).matchData.toSeq.headOption.flatMap { m =>
      try {
        // the xml should be under a sibling of the parent directory of this jar
        val base = Paths.get(path).getParent.getParent
        val found = FileSystems.entries(base, recursive = true).find(_.getFileName.toString == "pom.xml")
        found.flatMap { pomPath =>
          val pom = XML.loadFile(pomPath.toFile)
          new MavenDependencyEnumerator(pom, path).get
        }
      } catch {
        case e: Throwable =>
          throw new RuntimeException(s"XML parsing error from $path", e)
      }
    }
  }
}

class IvyPathArtifactResolver extends JarArtifactResolver[ManagedDependencyNode] {

  override def resolve(path: String): Option[ManagedDependencyNode] = {
    ivyPathFormat.findAllIn(path).matchData.toSeq.headOption.flatMap { m =>
      val group = m.group(1).replace('/', '.')
      val artifact = m.group(2)
      val filename = m.group(3)
      val version = filename.substring(artifact.length + 1, filename.length - 4)

      val ivy = Paths.get(path).getParent.getParent.resolve(s"ivy-$version.xml").toFile
      if (ivy.isFile && ivy.canRead) {
        val lines = Source.fromFile(ivy).getLines().mkString
        val xml = XML.loadString(ivyNameSpaceFormat.replaceAllIn(lines, ""))
        new IvyDependencyEnumerator(xml, path).get
      } else {
        logger.warn(s"Could not find Ivy XML corresponding to $path")
        None
      }
    }
  }
}


class IvyOriginalPathArtifactResolver extends JarArtifactResolver[ManagedDependencyNode] {
  override def resolve(path: String): Option[ManagedDependencyNode] = {
    ivyPathFormat.findAllIn(path).matchData.toSeq.headOption.flatMap { m =>
      val group = m.group(1).replace('/', '.')
      val artifact = m.group(2)
      val filename = m.group(3)
      val version = filename.substring(artifact.length + 1, filename.length - 4)

      try {
        val ivy = Paths.get(path).getParent.getParent.resolve(s"ivy-$version.xml.original").toFile
        if (ivy.isFile && ivy.canRead) {
          new MavenDependencyEnumerator(XML.loadFile(ivy), path).get
        } else {
          logger.warn(s"Could not find Ivy original XML corresponding to $path")
          None
        }
      } catch {
        case e: Throwable =>
          throw new RuntimeException(s"XML parsing error from $path", e)
      }
    }
  }
}

/** a catch-all resolver that resolves any JARs as Unmanaged */
class UnmanagedJarResolver extends JarArtifactResolver[UnmanagedDependencyNode] {
  override def resolve(path: String): Option[UnmanagedDependencyNode] = {
    Some(UnmanagedDependencyNode(path))
  }
}

/** chains multiple artifact resolvers and returns the first successful one */
class ChainedArtifactResolver[T <: DependencyNode](resolvers: JarArtifactResolver[T]*) extends JarArtifactResolver[T] {
  override def resolve(path: String): Option[T] = {
    resolvers.toStream.flatMap(r => Try(r.resolve(path)).toOption).collectFirst {
      case Some(node) => node
    }
  }
}
