package com.kakao.cuesheet.deps

import java.net.{URL, URLClassLoader}

import com.kakao.mango.concurrent.KeyedSingletons
import com.kakao.mango.logging.Logging
import com.kakao.mango.reflect.Accessible

import scala.reflect.io.AbstractFile

class DependencyAnalyzer(loader: ClassLoader = getClass.getClassLoader) extends Logging {
  val chain: List[ClassLoader] = DependencyAnalyzer.classLoaderChain(loader)

  lazy val graph = {
    // collect all nodes
    val nodes = for (
      loader <- chain;
      url <- DependencyAnalyzer.components(loader)
    ) yield {
      DependencyNode.resolve(url)
    }
    new DependencyGraph(nodes)
  }
}

object DependencyAnalyzer extends KeyedSingletons[ClassLoader, DependencyAnalyzer] with Logging {

  /** return given classloader and its ancestors as a list */
  def classLoaderChain(loader: ClassLoader = getClass.getClassLoader): List[ClassLoader] = {
    if (loader == null) Nil else loader :: classLoaderChain(loader.getParent)
  }

  /** return the list of URLs contained in the classloader */
  def components(loader: ClassLoader): Seq[URL] = {
    loader match {
      case _ if loader.getClass.getName.startsWith("sun.misc.Launcher$ExtClassLoader") =>
        Nil // ignore extension class loader
      case loader: URLClassLoader =>
        loader.getURLs
      case _ if loader.getClass.getName == "scala.tools.nsc.interpreter.AbstractFileClassLoader" =>
        val root = Accessible.field(loader.getClass, "root")
        Seq(root.get(loader).asInstanceOf[AbstractFile].toURL)
      case _ if loader.getClass.getName == "scala.reflect.internal.util.AbstractFileClassLoader" =>
        val root = Accessible.field(loader.getClass, "root")
        Seq(root.get(loader).asInstanceOf[AbstractFile].toURL)
      case _ if Seq(loader.getClass.getName).exists(c => c.startsWith("xsbt.") || c.startsWith("sbt.")) =>
        Nil // ignore SBT's internal loader
      case _ =>
        throw new RuntimeException("Unknown ClassLoader Type: " + loader.getClass.getName)
    }
  }

  override def newInstance(loader: ClassLoader): DependencyAnalyzer = {
    new DependencyAnalyzer(loader)
  }

  def apply(): DependencyAnalyzer = apply(getClass.getClassLoader)

}
