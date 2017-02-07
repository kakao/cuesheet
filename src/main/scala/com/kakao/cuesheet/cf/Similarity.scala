package com.kakao.cuesheet.cf

/** A common trait for calculating Cosine, Jaccard, Dice similarity measures */
trait SimilarityMeasure {

  /** Take powers between 0 to 2 to nonzero components to find the norm */
  def powered(a: Double): Double

  /** return the norm, when the sume of powered numbers are given */
  def norm(poweredSum: Double): Double

  /** combine nonzero components corresponding to the same dimension */
  def aggregate(a: Double, b: Double): Double

  /** find the actual similarity score based on the above numbers */
  def similarity(aggregateSum: Double, normA: Double, normB: Double): Double

}

case object CosineSimilarity extends SimilarityMeasure {

  override def powered(a: Double): Double = a * a

  override def norm(poweredSum: Double): Double = Math.sqrt(poweredSum)

  override def aggregate(a: Double, b: Double): Double = a * b

  override def similarity(aggregateSum: Double, normA: Double, normB: Double): Double = aggregateSum / (normA * normB)
}

trait CountBasedSimilarity extends SimilarityMeasure {

  override def powered(a: Double): Double = 1.0

  override def norm(poweredSum: Double): Double = poweredSum

  override def aggregate(a: Double, b: Double): Double = 1.0

}

case object JaccardSimilarity extends CountBasedSimilarity {
  override def similarity(common: Double, sizeA: Double, sizeB: Double): Double = common / (sizeA + sizeB - common)
}

case object DiceSimilarity extends CountBasedSimilarity {
  override def similarity(common: Double, sizeA: Double, sizeB: Double): Double = common / (sizeA + sizeB)
}


