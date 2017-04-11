package knnTesting


import knn._
import org.apache.spark.rdd.RDD
import org.scalatest._
import org.scalatest.mock.EasyMockSugar

/**
  * Created by The Boss on 4/10/2017.
  */
class integrationTest extends FlatSpec with Matchers with EasyMockSugar {

  "Mins and Maxes should be set based on mapping stuff" should "return min and max" in {

    val data: RDD[IrisPoint] = mock[RDD[IrisPoint]]
    val otherData: RDD[Double] = mock[RDD[Double]]

    expecting {
      data.map(IrisPointAccessorsFactoryThisSucks.getX).andReturn(otherData)
      otherData.max().andReturn(3.0)
      data.map(IrisPointAccessorsFactoryThisSucks.getY).andReturn(otherData)
      otherData.max().andReturn(1.0)
      data.map(IrisPointAccessorsFactoryThisSucks.getX).andReturn(otherData)
      otherData.min().andReturn(6.0)
      data.map(IrisPointAccessorsFactoryThisSucks.getY).andReturn(otherData)
      otherData.min().andReturn(4.0)
    }

    whenExecuting(data, otherData) {
      EntryPoint.getMinMaxes(data)
      knn.kNN.xMax should be (3.0)
    }
  }
}
