package org.apache.spark.streaming.kafka

/** a wrapper to expose package-private [[KafkaCluster]] */
object KafkaClusterHook {

  def newCluster(kafkaParams: Map[String, String]): KafkaCluster = new KafkaCluster(kafkaParams)

}
