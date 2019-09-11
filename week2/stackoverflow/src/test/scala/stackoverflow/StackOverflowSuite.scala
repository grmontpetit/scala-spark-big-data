package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import org.junit.Ignore

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("cluster results should be accurate") {
//    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StackOverflow")
//    @transient lazy val sc: SparkContext = new SparkContext(conf)
//    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
//    val raw     = testObject.rawPostings(lines)
//    val grouped = testObject.groupedPostings(raw)
//    val scored  = testObject.scoredPostings(grouped)
//    val vectors = testObject.vectorPostings(scored)
//    val means   = testObject.kmeans(testObject.sampleVectors(vectors), vectors, debug = true)
//    val results = testObject.clusterResults(means, vectors)
  }

  /* Below are some refactoring tests as the original code was not functional (ie, some functions had vars, etc...) */

  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  test("Returned index tag should be accurate") {
    val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      tag.fold[Option[Int]](None){ t =>
        Some(ls.indexOf(t))
      }
    }

    assert(firstLangInTag(Some("C++"), langs).contains(5))
  }

  test("check eucledian distance between 2 points") {

    val pts1 = Array((1, 2))
    val pts2 = Array((4, 5))

    def eucl1(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
      assert(a1.length == a2.length)
      a1.indices.foldLeft[Double](0.0){(acc, i) =>
        acc + euclideanDistance(a1(i), a2(i))
      }
    }

    def eucl2(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
      var sum = 0d
      var idx = 0
      while(idx < a1.length) {
        sum += euclideanDistance(a1(idx), a2(idx))
        idx += 1
      }
      sum
    }

    val d1 = eucl1(pts1, pts2)
    val d2 = eucl2(pts1, pts2)
    assert(d1 == d2, s"got $d1 want $d2")
  }

  test("closest point should be the same") {

    val centers = Array((1, 2), (6, 7))
    val point = (7, 7)

    def findClosest1(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
      var bestIndex = 0
      var closest = Double.PositiveInfinity
      for (i <- 0 until centers.length) {
        val tempDist = euclideanDistance(p, centers(i))
        if (tempDist < closest) {
          closest = tempDist
          bestIndex = i
        }
      }
      bestIndex
    }

    def findClosest2(p: (Int, Int), centers: Array[(Int, Int)]): Int = {

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

    val c1 = findClosest1(point, centers)
    val c2 = findClosest2(point, centers)

    println(c1, c2)
    assert(c1 == c2, s"got $c1 want $c2")
  }

  test("refactor average vectors") {

    val vectors = Iterable((1, 1), (2, 3), (8, 9), (4, 9))

    def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
      val iter = ps.iterator
      var count = 0
      var comp1: Long = 0
      var comp2: Long = 0
      while (iter.hasNext) {
        val item = iter.next
        comp1 += item._1
        comp2 += item._2
        count += 1
      }
      ((comp1 / count).toInt, (comp2 / count).toInt)
    }

    def averageVectors2(ps: Iterable[(Int, Int)]): (Int, Int) = {
      val (comp1, comp2, count) = ps.foldLeft((0L, 0L, 0)){(acc, tuple) =>
        val x = acc._1
        val y = acc._2
        val counter = acc._3
        (x + tuple._1, y + tuple._2, counter + 1)
      }
      ((comp1 / count).toInt, (comp2 / count).toInt)
    }

    val expected = averageVectors(vectors)
    val results = averageVectors2(vectors)
    assert(results == expected, s"got $results want $expected")
  }
}
