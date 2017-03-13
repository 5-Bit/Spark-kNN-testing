package knnTesting

import org.scalatest._
import knn._

class kNNTest extends FlatSpec with Matchers {

  "The knn module's distance function" should "test some stuff" in {
    knn.kNN.xMax = 3.0
    knn.kNN.xMax should be (3.0)
  }

  "A circle at (3,3) with radius (4) " should "overlap with a line from (3,3) to (20,20)" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(3,3,4),
      new knn.LineSegment(3,3,20,20)
    ) should be (true)
  }

  "A circle and line" should "overlap" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(1,1,5),
      new knn.LineSegment(1,1.1,3,4)
    ) should be (true)
  }



}