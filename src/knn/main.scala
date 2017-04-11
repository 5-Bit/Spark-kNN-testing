package knn
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashSet}

  object EntryPoint {

    var ConfHook: Option[SparkConf] = None
    var ContextHook: Option[SparkContext] = None

    // TODO:

    def getMinMaxes(data: RDD[IrisPoint]): Unit = {
      kNN.xMax = data.map(IrisPointAccessorsFactoryThisSucks.getX).max()
      kNN.yMax = data.map(IrisPointAccessorsFactoryThisSucks.getY).max()
      kNN.xMin = data.map(IrisPointAccessorsFactoryThisSucks.getX).min()
      kNN.yMin = data.map(IrisPointAccessorsFactoryThisSucks.getY).min()
    }

    def main(args: Array[String]): Unit = {

      val sConf = ConfHook.getOrElse(new SparkConf().setAppName("Spark").setMaster("local[*]")) // Init spark context
      val sc = ContextHook.getOrElse(new SparkContext(sConf)) // Init spark context

      val K = sc.broadcast(5) // Or something from the command line. :/
      val DIM_CELLS = sc.broadcast(kNN.DIM_CELLS) // The number of cells in each dimension

      // TODO: Build import code
      val irisData: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(x => Import.rowOfStr(x))

      // Built solely for the purpose of writing an integration test.
      // I hope you're happy with this, Prof Buckley.
      getMinMaxes(irisData)

      // pid, x, y, class

      // Assume normalized data, that will be done at some point :D
      // TODO: Build custom partitioner here?
      val cells = irisData.keyBy(kNN.pointToCellID).persist()

      val cellCounts: Broadcast[Map[Long, Long]] = sc.broadcast(cells.countByKey())
      overlapping.cellCounts = cells.countByKey()

      val testRecords: RDD[IrisPoint] = sc.textFile("data/iris_test_pid.csv").map(Import.rowOfStr)

      val keyedTestRecords: RDD[(Long, IrisPoint)] = testRecords.keyBy(kNN.pointToCellID)

      // val otherTestingStuff = keyedTestRecords.count()
      // Pass to find inital KNNs, and to calculate point-eqidistant bounding geometry
      val bucketedRecords: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] = keyedTestRecords.cogroup(cells) // TODO: Maybe persist here???

      val full: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] =
        bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
        val key = cell._1
        cellCounts.value.getOrElse(key, 0L) >= K.value
      })

      val splitData = full.mapPartitions(part => {
        val completedKNN: ArrayBuffer[IrisClassificationResult] = new ArrayBuffer()
        val overlappingKNN: ArrayBuffer[(Long, IrisPoint)] = new ArrayBuffer()
        for (cell <- part) {
          val testPoints = cell._2._1
          val trainPoints = cell._2._2

          val kPoints = kNN.knn(K.value, testPoints.toList, trainPoints.toList)

          for (twerp <- kPoints) {
            val center = twerp._1
            val neighbors = twerp._2
            val farthestPoint = twerp._2(K.value - 1)

            //
            val dist = knn.kNN.distance(center, farthestPoint)
            val overlapped = overlapping.GetIds(dist, (center.x, center.y))
            if (overlapped.length > 1L) {
              // Add all the stuff
              for (ever <- overlapped) {
                // overlappingKNN += ((ever, center))
                overlappingKNN.append((ever, center))
              }
            } else {
              val predicted = kNN.predictedOf(neighbors)
              completedKNN.append(new IrisClassificationResult(center.pid, center.classification, predicted))
            }
            // overlapping.CountIds((cetn))
          }
        }
        Array((completedKNN.toList, overlappingKNN.toList)).iterator
      })

      // Get all the classifications that don't overlap in any way.
      val stage1Results: List[IrisClassificationResult] = splitData.
        map((x: (List[IrisClassificationResult], List[(Long, IrisPoint)])) => x._1).reduce((x, y) => x ++ y)

      val stage2Data = splitData.flatMap(x => x._2).persist()

      val stage2Grouped: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] = stage2Data.cogroup(cells)

      val stage2Results: Array[IrisClassificationResult] = knn.kNN.runKnnOnOverlappingData(K.value, stage2Grouped)

      val needsAdditionalData: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] =
        bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
        val key = cell._1
        // Fishing out the testIter
        val testIter = cell._2._1
        cellCounts.value.getOrElse(key, 0L) < K.value && testIter.count(p => true) > 0
      })
      //// Pass to take points in empty/under-k cells, and build data
      //// to make a pass over the closest cells with data, containing
      //// all the data

      val cellIdsAndPidsToAddCheckTo: RDD[(Long, IrisPoint)] =
      needsAdditionalData.flatMap(x => {
        // Get cell Ids to check to get enough data...
        val cellID: Long = x._1
        // ...
        // Pull out the test records
        val records: Iterable[IrisPoint] = x._2._1

        val cellIds = new ArrayBuffer[(Long, IrisPoint)]()
        for (record <- records) {
          var radius: Double = kNN.cell_width() // Start off with a square-unit radius
          val center = (record.x, record.y)
          var enclosedIdCount = cellCounts.value.getOrElse(cellID, 0L)
          while (enclosedIdCount < K.value) {
            // Expand
            radius += 0.5 / DIM_CELLS.value
            enclosedIdCount = overlapping.CountIds(radius, center)
          }
          for (id <- overlapping.GetIds(radius, center)) {
            cellIds.append((id, record))
          }
        }
        cellIds
      })

      val stage3Grouped: RDD[(Long, (Iterable[(IrisPoint)], Iterable[IrisPoint]))] = cellIdsAndPidsToAddCheckTo.cogroup(cells);
      val stage3Results = knn.kNN.runKnnOnOverlappingData(K.value, stage3Grouped)

      println(stage1Results.length)
      println(stage2Results.length)
      println(stage3Results.length)

      val accuracy = (stage1Results ++ stage2Results ++ stage3Results).foldLeft((0, 0)) {
         (acc, res) => {
           if (res.actualClass == res.predictedClass) (acc._1 + 1 , acc._2)
           else (acc._1, acc._2 + 1)
         }
      }
      println(accuracy)
    }

  }

