package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int,
                   id: Int,
                   acceptedAnswer: Option[Int],
                   parentId: Option[QID],
                   score: Int,
                   tags: Option[String]) extends Serializable

/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {
    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })

  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings
      .filter(_.postingType == 1).map(q => (q.id, q))
    val answers = postings
      .filter(p => p.postingType == 2 && p.parentId.isDefined)
      .map(d => (d.parentId.get, d))
    val joined = questions.join(answers)
    joined.groupByKey()
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {
    def answerHighScore(as: Iterable[Answer]): HighScore = as.map(_.score).max
    grouped
      .flatMap(_._2)
      .groupByKey()
      .mapValues(answerHighScore)
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      tag.fold[Option[Int]](None){ t =>
        Some(ls.indexOf(t))
      }
    }
    scored
      .map(x => (firstLangInTag(x._1.tags, langs).getOrElse(0) * langSpread, x._2)).cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    vectors
      .map(v => (findClosest(v, means), v))
      .groupByKey
      .mapValues(averageVectors)
      .collect
      .foreach { case (index, averageMean) =>
        newMeans(index) = averageMean
      }

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }

  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean = distance < kmeansEta

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two array of points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    a1.indices.foldLeft[Double](0.0){(acc, i) =>
      acc + (euclideanDistance(a1(i), a2(i)))
    }
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    @tailrec
    def iterate(lastClosest: Double, currentIndex: Int, lastClosestIndex: Int, remaining: Array[(Int, Int)]): Int = {
      if (remaining.length == 0) lastClosestIndex
      else {
        val currentDistance = euclideanDistance(p, remaining.head)
        if (currentDistance < lastClosest)
          iterate(currentDistance, currentIndex + 1, currentIndex, remaining.tail)
        else {
          iterate(lastClosest, currentIndex + 1, lastClosestIndex, remaining.tail)
        }
      }
    }
    iterate(Int.MaxValue, 0, 0, centers)
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val (comp1, comp2, count) = ps.foldLeft((0L, 0L, 0)){(acc, tuple) =>
      val x = acc._1
      val y = acc._2
      val counter = acc._3
      (x + tuple._1, y + tuple._2, counter + 1)
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest: RDD[(Int, (LangIndex, HighScore))] = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped: RDD[(Int, Iterable[(LangIndex, HighScore)])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>

      val grouped = vs
        .map(_._1 / langSpread)
        .groupBy(identity)
        .mapValues(_.size)

      // most common language in the cluster
      val maxIndex = grouped.maxBy(_._2)._1
      val langLabel: String = langs(maxIndex)
      // percent of the questions in the most common language
      val langPercent: Double = (grouped(maxIndex).toDouble / vs.size.toDouble) * 100.toDouble
      val clusterSize: Int = vs.size
      val (left, right) = vs.map(_._2).toList.sorted.splitAt(clusterSize / 2)
      val medianScore: Int = if (clusterSize % 2 > 0) right.head else (left.last + right.head) / 2

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
