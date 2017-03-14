package knnTesting

import org.scalatest._
import knn._

class geometryTest extends FlatSpec with Matchers {

  "A circle at (3,3) with radius (4) " should "overlap with a line from (3,3) to (20,20)" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(3,3,4),
      new knn.LineSegment(3,3,20,20)
    ) should be (true)
  }

  "A circle at (1,1) with radius 5 and line from (1,1.1) to (3,4)" should
  "overlap" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(1,1,5),
      new knn.LineSegment(1,1.1,3,4)
    ) should be (true)
  }

  "A circle at (5, 7) with radius 4.5 and a line from (20, 24) to (-20, 23) " should
  "not overlap" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(5, 7, 4.5),
      new knn.LineSegment(3, 31, 23, -13)
    )  should be (false)
  }
  "THIS" should "BE FALSE" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(0, 0, 3),
      new knn.LineSegment(0, 31, 31, 0)
    )  should be (false)

  }

  /*
  x1: 3 y1: 31

  x2: 23 y2: -13
  */

  "A circle centered at (3,6) and a line from (5,6) to (12, 1337)" should
  "have a closest point of (5,6)" in {
    val expected = new Vector2D(5, 6)
    val actual = knn.overlapping.closestPointOnSeg(
      new Vector2D(5, 6),
      new Vector2D(12, 1337),
      new Vector2D(3, 6)
    )
    assert(expected == actual)
  }

  "A circle centered at (42, 69) and a line from (43, 69) to (45, 0xYOURMOM) " should
  "Have a distance of 1" in {

  }


  // TODO: Figure out more tests here.

}