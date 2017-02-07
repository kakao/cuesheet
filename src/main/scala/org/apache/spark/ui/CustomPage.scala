package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

abstract class CustomPage(val name: String) extends WebUIPage("") {

  private[ui] var tab: SparkUITab = _
  private[ui] var title: String = _
  private[ui] var ui: SparkUI = _

  def attachRedirectHandler(
    srcPath: String,
    destPath: String,
    beforeRedirect: HttpServletRequest => Unit = x => (),
    basePath: String = "",
    httpMethods: Set[String] = Set("GET")): Unit = {

    // Can't use Jetty interface as it is shaded to org.spark-project; use reflection instead
    val createRedirectHandler = JettyUtils.getClass.getMethods.filter(_.getName == "createRedirectHandler").head
    val handler = createRedirectHandler.invoke(JettyUtils, srcPath, destPath, beforeRedirect, basePath, httpMethods)

    val attachHandler = ui.getClass.getMethods.filter(_.getName == "attachHandler").head
    attachHandler.invoke(ui, handler)
  }

  def renderPage(request: HttpServletRequest): Seq[Node]

  final override def render(request: HttpServletRequest): Seq[Node] = {
    val content = renderPage(request)
    UIUtils.headerSparkPage(title, content, tab, Some(5000))
  }

}
