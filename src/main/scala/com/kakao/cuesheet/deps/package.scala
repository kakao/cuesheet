package com.kakao.cuesheet

import java.io.File

import scala.util.matching.Regex
import scala.xml.{NamespaceBinding, TopScope}

/** This package is where a variety of JVM tricks are used to enumerating the runtime dependencies
  * This package object contains global variables used in this package, mostly for text processing.
  */
package object deps {

  private val sep = if (File.separatorChar == '/') """\/""" else """\\"""

  val (mavenPathFormat, gradlePathFormat, ivyPathFormat) = if (File.separatorChar == '/') (
    """\.m2\/repository\/([\w\/\-\_]*)\/([\w\.\-]+)\/([\w\.\-\_]+)\/([\w\.\-\_]+\.jar)$""".r,
    """\.gradle\/caches\/[\w\-\.]+\/[\w\-\.]+\/([\w\/\-\_\.]*)\/([\w\.\-]+)\/([\w\.\-\_]+)\/[\w]+\/([\w\.\-\_]+\.jar)$""".r,
    """\.ivy2\/cache\/([\w\.\-]+)\/([\w\.\-\_]+)\/[\w]+\/([\w\.\-\_]+\.jar)$""".r
  ) else (
    """\.m2\\repository\\([\w\\\-\_]*)\\([\w\.\-]+)\\([\w\.\-\_]+)\\([\w\.\-\_]+\.jar)$""".r,
    """\.gradle\\caches\\[\w\-\.]+\\[\w\-\.]+\\([\w\\\-\_\.]*)\\([\w\.\-]+)\\([\w\.\-\_]+)\\[\w]+\\([\w\.\-\_]+\.jar)$""".r,
    """\.ivy2\\cache\\([\w\.\-]+)\\([\w\.\-\_]+)\\[\w]+\\([\w\.\-\_]+\.jar)$""".r
  )

  val ivyNameSpaceFormat: Regex = """<(e|m):[\w\.\-\_]+>[\$\{\}\w\.\n\-\W]+<\/(e|m):[\w\.\-\_]+>""".r
  val ivyMavenNB = NamespaceBinding("m", "http://ant.apache.org/ivy/maven", TopScope)
  val ivyExtraNB = NamespaceBinding("e", "http://ant.apache.org/ivy/extra", TopScope)

  val propertyKeyFormat: Regex = """\$\{([\w\.\-\_]+)\}""".r

  /** when zipping directory, only those files having the following extensions will be included */
  val resourceExtensions = Seq(".class", ".conf", ".properties", ".xml", ".tsv", ".csv", ".txt")
}
