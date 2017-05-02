package knn
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object kNN {

  var DIM_CELLS = 10; // The number of cells in each dimension

  var xMax = 0.0
  var yMax = 0.0
  var xMin = 0.0
  var yMin = 0.0

  def cell_width(): Double = { (xMax - xMin) / DIM_CELLS.toDouble }
  def cell_height(): Double = { (yMax - yMin) / DIM_CELLS.toDouble }

  def distance(point1: IrisPoint, point2: IrisPoint): Double = {
    math.sqrt(
      math.pow(math.abs(point1.x - point2.x), 2)
      + math.pow(math.abs(point1.y - point2.y), 2)
      + math.pow(math.abs(point1.z - point2.z), 2)
      + math.pow(math.abs(point1.w - point2.w), 2)
    )
  }

  def knn(k: Int, test: Iterable[IrisPoint], train: List[IrisPoint]): List[(IrisPoint, List[IrisPoint])] = {
    test.map(testRecord => {
      var neighbors = new ArrayBuffer[IrisPoint](k)
      for (tr <- train) {
        neighbors.append(tr)
        neighbors.sortWith((r,l) => { distance(testRecord, r) < distance(testRecord, l) })
        if (neighbors.length > k) {
          neighbors.reduceToSize(k)
        }
      }

      (testRecord, neighbors.toList)
    }).toList
  }

  // TODO: Test this
  def mergeKnn(k: Int, testPoint: IrisPoint, trainLeft: List[IrisPoint], trainRight: List[IrisPoint]): (IrisPoint, List[IrisPoint]) = {
    (testPoint,
      // Merge the right and left lists
      (trainLeft ++ trainRight).
        sortWith((l, r) => distance(testPoint, l) < distance(testPoint, r))
        .take(k))
  }

  // TODO: test this in the full integration testy thingy
  def runKnnOnOverlappingData(k: Int, data: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))]): Array[IrisClassificationResult] = {
    data.mapPartitions(part => {
      part.flatMap( cell => {
        val cell_id:Long = cell._1
        val data = cell._2

        val test = data._1
        val train = data._2
        kNN.knn(k, test.toList, train.toList)
      })
    }).
      keyBy(x => x._1.pid).
      reduceByKey((l, r) => { kNN.mergeKnn(k, l._1, l._2, r._2) })
      .aggregateByKey(new IrisClassificationResult(-1, "", "")) ({
        (acc, value) => {
          new IrisClassificationResult(value._1.pid, value._1.classification, kNN.predictedOf(value._2))
        }
      }, { (l, r) => r }).map(x => x._2).collect()
  }

  def predictedOf(records: List[IrisPoint]): String = {
      // Extract the classifications
      records.map(n => n.classification).
      // Count them into groups
      foldLeft(Map[String, Int]()) {
        (acc, elem) => { acc + ((elem, acc.getOrElse(elem, 1) + 1)) }
      }
        // Grap the group with the largest count
      .reduce {
        (current, candidate) => if (current._2 < candidate._2) candidate else current
      }._1 // Peel the classificaiton out of the tuple
  }

  def xyToCellId(x:Double, y:Double): Long = {
    val x_val = math.floor((x - xMin) / cell_width()).toInt
    val y_val = math.floor((y - yMin) / cell_width()).toInt

    // Lineralize the list of cell ids
    val cell_id = (DIM_CELLS * y_val) + x_val
    cell_id
  }

  def pointToCellID(row:IrisPoint): Long = {
    xyToCellId(row.x, row.y)
  }
}
