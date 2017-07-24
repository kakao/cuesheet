package org.apache.spark.ui

import com.google.common.base.CaseFormat._
import org.apache.spark.{SparkContext, SparkException}

case class SparkUIHook(sc: SparkContext) {

  def add(page: CustomPage) = {
    val ui = sc.ui.getOrElse { throw new SparkException("No Spark UI found!") }

    page.title = LOWER_HYPHEN.to(UPPER_CAMEL, page.name).replaceAll("[A-Z]", " $0").trim
    page.tab = new SparkUITab(ui, page.name) {}
    page.tab.attachPage(page)
    page.ui = ui

    ui.attachTab(page.tab)
  }
  
  def getUIAddress: String = {
    val ui = sc.ui.getOrElse { throw new SparkException("No Spark UI found!") }
    ui.webUrl
  }

}
