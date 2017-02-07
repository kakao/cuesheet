package com.kakao.cuesheet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.ha.ZKFailoverController
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil

import scala.collection.JavaConversions._

object HadoopUtils {

  /** The Hadoop configuration keys for [[getHadoopXML]] */
  val defaultKeys = Seq(
    CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
    ZKFailoverController.ZK_QUORUM_KEY,
    DFSConfigKeys.DFS_NAMESERVICES,
    DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX,
    DFSConfigKeys.DFS_CLIENT_FAILOVER_PROXY_PROVIDER_KEY_PREFIX,
    DFSConfigKeys.DFS_HA_AUTO_FAILOVER_ENABLED_KEY,
    DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY
  )

  /** Minimal XML to be able to connect to HDFS */
  def getHadoopXML(conf: Configuration): String = {
    YarnSparkHadoopUtil.escapeForShell(<configuration>{
      conf.iterator().collect {
        case entry if defaultKeys.exists(entry.getKey.startsWith) =>
          <property><name>{entry.getKey}</name><value>{entry.getValue}</value></property>
      }
    }</configuration>.toString())
  }

}
