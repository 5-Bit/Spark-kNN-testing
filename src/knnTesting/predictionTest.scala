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

}
