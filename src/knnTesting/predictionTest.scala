package knnTesting

import knn._
import org.scalatest._

class predictionTest extends FlatSpec with Matchers {
  val versicolorPoints =
    Array("Iris-setosa", "Iris-versicolor", "Iris-versicolor", "Iris-virginica")
      .map(classifcation => new IrisPoint(0,0,0,0,0,classifcation)).toList

  val setosapoints =
    Array("Iris-setosa", "Iris-setosa", "Iris-versicolor", "Iris-virginica")
      .map(classifcation => new IrisPoint(0,0,0,0,0,classifcation)).toList
  // TODO: Add detection for other types of kNN predictions, such as distance weighted ones
  "A majority of a given test" should "create a classification" in {
    knn.kNN.predictedOf(versicolorPoints) should be ("Iris-versicolor")
    knn.kNN.predictedOf(setosapoints) should be ("Iris-setosa")
  }

  "An iris point " should "load from a string correctly" in {
    val pt = Import.rowOfStr("2,1.0,2.0,3.0,4.0,Class")
    pt.x should be (3.0)
    pt.y should be (4.0)
    pt.w should be (2.0)
    pt.z should be (1.0)
    pt.classification should be ("Class")
  }

  "Testing and training data " should "train correctly" in {

    val pfoo = new IrisPoint(0, 1,1,1,3,"foo")
    val prest = new IrisPoint(3, 5,5,5,5,"rest")
    val pquux =  new IrisPoint(6, 8,8,8,8,"quux")

    val close_foo = new IrisPoint(0, 1,1,1,3,"foo")
    val close_rest = new IrisPoint(3, 5,5,5,5,"rest")

    val points = List(
      // Proto point A: 1/1/1/1/foo
      close_foo,
      new IrisPoint(1, 1,2,1,1,"foo"),
      new IrisPoint(2, 3,1,1,4,"foo"),

      // Proto point B: 5/5/5/5/rest
      close_rest,
      new IrisPoint(4, 5,4,5,7,"rest"),
      new IrisPoint(5, 9,5,3,5,"rest"),
      // Proto point c: 8/8/8/8/quux
      new IrisPoint(6, 8,6,8,8,"quux"),
      new IrisPoint(7, 8,8,9,8,"quux"),
      new IrisPoint(8, 10,8,8,10,"quux")
    )

    val result = knn.kNN.knn(2, List(pfoo, prest, pquux), points)

    result(0)._2(0) should be (close_foo)
    result(0)._1 should be (pfoo)

  }


}
