package knnTesting

import knn._
import org.scalatest._

class geometryTest extends FlatSpec with Matchers {

  "point to cell id" should "map linearly into a grid" in {
    kNN.DIM_CELLS = 4
    kNN.xMin = 0.0
    kNN.xMax = 5.0
    kNN.yMin = 0.0
    kNN.yMax = 5.0

    // I don't know why the first pass is bugged. Probably just a scala thing.
    kNN.pointToCellID(new IrisPoint(0, 0.5, 0.5, 0,0, "")) should be (0)
    kNN.pointToCellID(new IrisPoint(0, 2.5, 0.5, 0,0,"")) should be (2)
    kNN.pointToCellID(new IrisPoint(0, 1.5, 2.5, 0,0, "")) should be (9)
    kNN.pointToCellID(new IrisPoint(0, 4.5, 4.5, 0,0, "")) should be (15)
  }

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
    ) should be (false)
  }
  "A circle at (0,0) with radius 3 and a line from (0,31) to (31, 0)" should "not overlap" in {
    knn.overlapping.doesLineSegmentOverlapCircle(
      new knn.Circle(0, 0, 3),
      new knn.LineSegment(0, 31, 31, 0)
    ) should be (false)
  }

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

  "A circle centered at (0, 0) with a radius of 4 and a line from (3, 4) to (12, 1337) " should
  "Have a distance of 4.0" in {
    val expectedDist: Double = 4.0

    val actual = knn.overlapping.segmentCircleDist(
      new Vector2D(3, 4),
      new Vector2D(12, 1337),
      new Vector2D(0, 0),
      1
    )
    assert(expectedDist == actual)
  }

  "A circle centered at (0, 0) with a radius of 4 and a line from (12, 1337) to (3, 4) " should
    "Have a distance of 4.0" in {
    val expectedDist: Double = 4.0

    val actual = knn.overlapping.segmentCircleDist(
      new Vector2D(12, 1337),
      new Vector2D(3, 4),
      new Vector2D(0, 0),
      1
    )
    assert(expectedDist == actual)
  }

  "A circle and line that share a point" should "have zero distance" in {
    knn.overlapping.segmentCircleDist(
      new Vector2D(2,2),
      new Vector2D(3,4),
      new Vector2D(2,2),
      500
    ) should be (0.0)
  }

  "A zero-length line" should "behave like a point" in {
    knn.overlapping.segmentCircleDist(
      new Vector2D(2,2),
      new Vector2D(2,2),
      new Vector2D(5,6),
      500
    ) should be (495.0)
  }

  "(1,2,3,4) " should "be x away from (4,5,6,7)" in {
    val r = new IrisPoint(0,1,2,3,4,"foo")
    val l = new IrisPoint(0,4,5,6,7,"bar")
    // TODO: Fix this
    knn.kNN.distance(r, l) should be (6.0)
  }

  // This map is a copy of data created from the iris dataset.
  overlapping.cellCounts = Map[Long, Long]( 2L -> 20L, 3L -> 10L, 5L -> 10L, 11L -> 10L, 12L -> 10L, 13L -> 40L, 14L -> 20L, 15L -> 20L, 17L -> 10L, 20L -> 40L, 21L -> 20L, 22L -> 20L, 23L -> 60L, 24L -> 100L, 25L -> 70L, 26L -> 30L, 27L -> 20L, 28L -> 10L, 29L -> 10L, 30L -> 20L, 31L -> 50L, 32L -> 20L, 34L -> 10L, 35L -> 10L, 37L -> 80L, 38L -> 10L, 40L -> 20L, 41L -> 20L, 42L -> 70L, 43L -> 30L, 45L -> 20L, 48L -> 10L, 52L -> 40L, 53L -> 30L, 54L -> 20L, 60L -> 10L)
  "Cell Analysis" should "display correct overlap values" in {
    kNN.DIM_CELLS = 10

    kNN.xMax = 7.7
    kNN.xMin = 4.3

    kNN.yMax = 4.0
    kNN.yMin = 2.0

    // println(kNN.cell_width())
    knn.overlapping.CountIds(0.1, (4.3, 2.1)) should be (0)
    knn.overlapping.CountIds(0.1, (4.3 + 0.34 + 0.34 + 0.20, 2.1)) should be (0)

  }
}