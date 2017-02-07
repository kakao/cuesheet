package com.kakao.cuesheet

import java.nio.file.{Files, Paths}

import com.kakao.cuesheet.deps.{DependencyAnalyzer, ManagedDependencyNode}
import com.kakao.mango.logging.Logging

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.Try

/** A heuristic to find CueSheet version in runtime */
object CueSheetVersion extends Logging {
  private val versionPattern = """[^"]*"([^"]+)".*""".r

  lazy val version: String = {
    // read from MANIFEST.MF
    getClass.getClassLoader.getResources("META-INF/MANIFEST.MF").toSeq.flatMap { url =>
      val src = Source.fromInputStream(url.openStream())
      try {
        val manifest = src.getLines().map(_.split(":", 2)).collect {
          case Array(key, value) => (key.trim(), value.trim())
        }.toMap
        (manifest.get("Implementation-Vendor"), manifest.get("Implementation-Title")) match {
          case (Some("com.kakao.cuesheet"), Some("cuesheet")) => manifest.get("Implementation-Version")
          case (Some("com.kakao.cuesheet"), Some("cuesheet-assembly")) => manifest.get("Implementation-Version")
          case _ => Nil
        }
      } finally {
        src.close()
      }
    }.headOption.orElse {
      val (_, applicationJars) = DependencyAnalyzer().graph.divide()
      applicationJars.collectFirst {
        case jar: ManagedDependencyNode if jar.artifact.startsWith("cuesheet") => jar.version
      }
    }.orElse {
      Try(Files.readAllBytes(Paths.get("version.sbt"))).map { bytes =>

      }.toOption
      Try(Source.fromFile("version.sbt")).map { src =>
        // try to read from version.sbt
        try {
          src.getLines().collectFirst {
            case versionPattern(v) => v
          }.head
        } finally {
          src.close()
        }
      }.toOption
    }.getOrElse("Unknown")
  }

}
