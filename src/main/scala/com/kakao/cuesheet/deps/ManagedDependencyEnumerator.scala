package com.kakao.cuesheet.deps

import java.io.File

import com.kakao.mango.io.FileSystems
import com.kakao.mango.logging.Logging

import scala.xml.{Elem, XML}

sealed trait ManagedDependencyEnumerator extends Logging {
  def get: Option[ManagedDependencyNode]
}

class MavenDependencyEnumerator(pom: Elem, path: String) extends ManagedDependencyEnumerator {
  override def get: Option[ManagedDependencyNode] = {
    val project = pom \\ "project"
    val parent = project \ "parent"

    val group = (project \ "groupId").headOption.orElse((parent \ "groupId").headOption).get.text
    val artifact = (project \ "artifactId").text
    val version = (project \ "version").headOption.orElse((parent \ "version").headOption).get.text

    val filename = new File(path).getName
    val expectedName = s"$artifact-$version.jar"
    val classifier = if (filename == expectedName) "jar" else {
      val substring = filename.substring(expectedName.length - 3, filename.length - 4)
      logger.debug(s"non-standard JAR classifier $substring is detected for $path")
      substring
    }

    val replacer = new MavenPropertyReplacer(pom, path, group, artifact, version)

    val children = (project \ "dependencies" \ "dependency").map { node =>
      ManagedDependency(
        replacer((node \ "groupId").text),
        replacer((node \ "artifactId").text),
        replacer((node \ "classifier").headOption.map(_.text).getOrElse("jar"))
      )
    }

    Some(ManagedDependencyNode(path, replacer(group), replacer(artifact), classifier, replacer(version), children))
  }
}

class MavenPropertyReplacer(pom: Elem, path: String, group: String, artifact: String, version: String) {
  private val project = pom \\ "project"

  val properties: Map[String, String] = ({
    (project \ "properties").head.child.map(p => p.label -> p.text)
  } ++ Seq(
    "project.groupId" -> group,
    "project.artifactId" -> artifact,
    "project.version" -> version
  )).toMap

  lazy val parent: Option[MavenPropertyReplacer] = {
    (project \ "parent").headOption.flatMap { parent =>
      val group = (parent \ "groupId").head.text
      val artifact = (parent \ "artifactId").head.text
      val version = (parent \ "version").headOption.orElse((project \ "version").headOption).get.text

      ivyPathFormat.findFirstMatchIn(path).map { m =>
        val prefix = path.substring(0, m.start)
        val ivyOriginal = s"$prefix.ivy2/cache/$group/$artifact/ivy-$version.xml.original"
        val pom = XML.loadFile(ivyOriginal)
        new MavenPropertyReplacer(pom, path, group, artifact, version)
      } orElse {
        mavenPathFormat.findFirstMatchIn(path).map { m =>
          val prefix = path.substring(0, m.start)
          val pomPath = s"$prefix.m2/repository/${group.replace('.', '/')}/$artifact/$version/${artifact}-$version.pom"
          val pom = XML.loadFile(pomPath)
          new MavenPropertyReplacer(pom, path, group, artifact, version)
        }
      } orElse {
        gradlePathFormat.findFirstMatchIn(path).flatMap { m =>
          val prefix = path.substring(0, m.start)
          val base = s"$prefix.gradle/caches/modules-2/files-2.1/$group/$artifact/$version/"
          FileSystems.entries(base).find(_.toString.endsWith(".pom")).map { pomPath =>
            val pom = XML.loadFile(pomPath.toString)
            new MavenPropertyReplacer(pom, path, group, artifact, version)
          }
        }
      }
    }
  }

  def getValue(key: String): Option[String] = {
    properties.get(key) orElse parent.flatMap(_.getValue(key))
  }

  def apply(input: String): String = {
    var string = input

    while (true) {
      propertyKeyFormat.findFirstMatchIn(string) match {
        case Some(m) =>
          val key = m.group(1)
          getValue(key) match {
            case Some(value) =>
              string = string.replace("${" + key + "}", value)
            case None =>
              // temporarily use '^' to mark that no replacement is available
              string = string.replace("${" + key + "}", "^{" + key + "}")
          }
        case None =>
          // no more stuff to replace
          return string.replace("^", "$")
      }
    }

    string
  }
}

class IvyDependencyEnumerator(ivy: Elem, path: String) extends ManagedDependencyEnumerator {
  override def get: Option[ManagedDependencyNode] = {
    val info = (ivy \\ "info").head

    val group = info.attribute("organisation").map(_.text).get
    val artifact = info.attribute("module").map(_.text).get
    val version = info.attribute("revision").map(_.text).get

    val children = (ivy \\ "dependencies" \ "dependency").flatMap { node =>
      // TODO: exclude test/provided scope
      val dependencyOrg = node.attribute("org").map(_.text)
      val dependencyName = node.attribute("name").map(_.text)

      val artifacts = node \ "artifact"
      if (artifacts.nonEmpty) {
        // one or more artifacts in this dependency
        for (artifact <- artifacts) yield {
          val artifactOrg = artifact.attribute("org").map(_.text).orElse(dependencyOrg).get
          val artifactName = artifact.attribute("name").map(_.text).orElse(dependencyName).get
          val artifactClassifier = artifact.attributes.get(ivyMavenNB.uri, ivyMavenNB, "classifier").map(_.text).getOrElse("jar")
          ManagedDependency(artifactOrg, artifactName, artifactClassifier)
        }
      } else {
        (dependencyOrg, dependencyName) match {
          case (Some(org), Some(name)) => Some(ManagedDependency(org, name, "jar"))
          case _ =>
            throw new IllegalArgumentException(s"dependency node $node does not have organisation or name")
        }
      }
    }

    Some(ManagedDependencyNode(path, group, artifact, "jar", version, children))
  }
}