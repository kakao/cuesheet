package com.kakao.cuesheet.cf

import com.kakao.mango.logging.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

object ItemBasedCF {

  def apply[USER: ClassTag, ITEM: ClassTag](implicit ordering: Ordering[ITEM]) = new ItemBasedCF[USER, ITEM]

  case class Options(
    /** do not include users with preference records fewer than this number */
    minPreferenceCountPerUser: Int = 3,

    /** ignore preference score less than this value */
    minPreferenceThreshold: Double = 0.0,

    /** consider at most this many similar items */
    maxSimilarItems: Int = 10,

    /** ignore items with similarity score less than this */
    minSimilarityThreshold: Double = 0.1,

    /** limit the size of final recommendation by this number */
    maxRecommendations: Int = 100
  )

  implicit val defaultOptions: Options = Options()

  implicit def strip[T](b: Broadcast[T]): T = b.value

}


class ItemBasedCF[USER: ClassTag, ITEM: ClassTag](implicit ordering: Ordering[ITEM]) extends Logging {

  import ItemBasedCF._

  /** user => (preferred item, preference score) */
  type Preferences = RDD[(USER, (ITEM, Double))]

  /** user => Seq of (recommended item, score) */
  type Recommendations = RDD[(USER, Seq[(ITEM, Double)])]

  /** ((item1, item2), similarity) */
  type Similarities = RDD[((ITEM, ITEM), Double)]

  def preprocess(prefs: Preferences)(implicit options: Options): Preferences = {
    import options._

    val sc = prefs.sparkContext

    // apply minPreferenceCountPerUser; should be fine with broadcasting, with about 10M users
    val users = sc.broadcast(prefs.countByKey().filter { case (_, count) => count >= minPreferenceCountPerUser}.keySet)

    // apply minPreferenceThreshold
    prefs.filter {
      case (user, (item, score)) => users.contains(user) && score > minPreferenceThreshold
    }
  }

  def similarity(prefs: Preferences, measure: SimilarityMeasure = CosineSimilarity): Similarities = {
    val sc = prefs.sparkContext
    val o = this.ordering

    // the norm for cosine similarity
    val norms = sc.broadcast(prefs.map {
      case (user, (item, score)) => (item, measure.powered(score))
    }.reduceByKey(_ + _).mapValues(measure.norm).collectAsMap())

    val similarities = (prefs join prefs)
      .flatMapValues {
        case ((item1, score1), (item2, score2)) if o.compare(item1, item2) < 0 => Some((item1, item2), score1 * score2)
        case _ => None
      }
      .values
      .reduceByKey { _ + _ }
      .map{ case ((item1, item2), dotProduct) => ((item1, item2), dotProduct / (norms(item1) * norms(item2))) }

    similarities
  }

  def predict(similarities: Similarities, pref: Preferences)(implicit options: Options): Recommendations = {
    val topSimilarItems = similarities.map { case ((item1, item2), score) => (item1, (item2, score))}
      .groupByKey()
      .mapValues {
      // for each item, sort its similar items ordered by the similarity scores
      items => items.toSeq.sortBy(-_._2).take(options.maxSimilarItems)
    }

    // for each item, users who like it and the preference score
    val itemScores = pref.map {
      case (user, (item, score)) => (item, (user, score))
    }.groupByKey()

    val result = itemScores.join(topSimilarItems).flatMap[(USER, (ITEM, Double))] {
      // multiply the user's preference score on an item with the similarity score of a similar item
      case (item, (prefItemScores, similarItemScores)) =>
        for {
          (user, prefScore) <- prefItemScores
          (similarItem, similarityScore) <- similarItemScores
        } yield (user, (similarItem, prefScore * similarityScore ))
    } // group by each user
      .groupByKey().mapValues[Seq[(ITEM, Double)]] {
      _ // merge the scores for same items
        .groupBy{ case (item, score) => item }
        .mapValues(_.map{ case (item, score) => score }.sum)
        // sort by the score
        .toSeq.sortBy{ case (item, score) => -score }
        // limit the result size
        .take(options.maxRecommendations)
    }

    result
  }

  def recommend(preferences: Preferences)(implicit options: Options): (Similarities, Recommendations) = {
    val prefs = preprocess(preferences)(options)
    val similarities = similarity(prefs)
    val recommendations = predict(similarities, prefs)(options)
    (similarities, recommendations)
  }

}
