package knnTesting

import knn._
import org.scalatest._

class functionalTest extends FlatSpec with Matchers {
  "The test dataset " should "have more than 90% accuracy" in {
    EntryPoint.main(Array(""))
    (EntryPoint.CorrectClassifications.toDouble / (EntryPoint.CorrectClassifications + EntryPoint.IncorrectClassifications) > 0.9) should be (true)
  }
}
