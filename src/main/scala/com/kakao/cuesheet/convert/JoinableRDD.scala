package com.kakao.cuesheet.convert

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class JoinableRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

  def selfJoin(numPartitions: Int = rdd.partitions.length): RDD[(K, (V, V))] = fastJoin(rdd, numPartitions)

  def fastJoin[W](other: RDD[(K, W)], numPartitions: Int = rdd.partitions.length): RDD[(K, (V, W))] = {
    val partitioner = new HashPartitioner(numPartitions)
    val grouped = rdd cogroup other

    val left = grouped.flatMap{
      case (k, (vs, ws)) => vs.zipWithIndex.map {
        case (v, idx) => ((k, idx), v)
      }
    }.partitionBy(partitioner)

    val right = grouped.flatMap {
      case (k, (vs, ws)) => ws.map { w => ((k, w.hashCode()), (w, vs.size)) }
    }.partitionBy(partitioner).flatMap {
      case ((k, r), (w, size)) => (0 until size).map(i => ((k, w), i))
    }.map {
      case ((k, w), idx) => ((k, idx), w)
    }

    (left join right).map {
      case ((k, idx), (v, w)) => (k, (v, w))
    }
  }

}
