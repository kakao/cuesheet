package com.kakao

package object cuesheet {

  val scalaVersion: String = util.Properties.versionNumberString.split("\\.").take(2).mkString(".")

  object DeployMode extends Enumeration {
    type DeployMode = Value
    val CLIENT, CLUSTER = Value
  }

  type DeployMode = DeployMode.DeployMode
  val CLIENT = DeployMode.CLIENT
  val CLUSTER = DeployMode.CLUSTER

  object ClusterManager extends Enumeration {
    type ClusterManager = Value
    val YARN, SPARK, MESOS, LOCAL = Value
  }
  
  type ClusterManager = ClusterManager.ClusterManager
  val YARN = ClusterManager.YARN
  val SPARK = ClusterManager.SPARK
  val MESOS = ClusterManager.MESOS
  val LOCAL = ClusterManager.LOCAL

  /** check if the top method in this thread's stack is Hadoop's */
  def isOnHadoop: Boolean = {
    val main = new Throwable().getStackTrace.last
    main.getClassName.startsWith("org.apache.hadoop")
  }

  /** check if the top method in this thread's stack is Spark's */
  def isOnSpark: Boolean = {
    val main = new Throwable().getStackTrace.last
    main.getClassName.startsWith("org.apache.spark")
  }

  def isOnCluster: Boolean = isOnHadoop || isOnSpark

}
