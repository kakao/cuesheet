package org.apache.spark.launcher

/** exposes non-public methods in spark-launcher */
object SparkLauncherHook {
  def quoteForCommandString(str: String): String = CommandBuilderUtils.quoteForCommandString(str)
}
