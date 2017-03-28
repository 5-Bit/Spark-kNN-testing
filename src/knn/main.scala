package knn
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashSet}

  object EntryPoint {

    def main(args: Array[String]): Unit = {

      val sConf = new SparkConf().setAppName("Spark").setMaster("local[*]") // Init spark context
      val sc = new SparkContext(sConf) // Init spark context
      println(System.getProperty("user.dir"))

      val K = sc.broadcast(5) // Or something from the command line. :/
      val DIM_CELLS = sc.broadcast(kNN.DIM_CELLS) // The number of cells in each dimension

      // TODO: Build import code
      val irisData: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(x => Import.rowOfStr(x))

      val x_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).max())
      val y_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).max())
      val x_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).min())
      val y_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).min())

      kNN.xMax = x_max.value
      kNN.yMax = y_max.value
      kNN.xMin = x_min.value
      kNN.yMin = y_min.value

      println(kNN.xMax)
      println(kNN.yMax)
      println(kNN.xMin)
      println(kNN.yMin)



      // pid, x, y, class

      // Assume normalized data, that will be done at some point :D
      // TODO: Build custom partitioner here?
      val cells = irisData.keyBy(kNN.pointToCellID).persist()

      val cellCounts: Broadcast[Map[Long, Long]] = sc.broadcast(cells.countByKey())
      overlapping.cellCounts = cells.countByKey()
      println(overlapping.cellCounts)

      // value is the count here.
      val fullCellCounts = cellCounts.value.filter(x => x._2 >= K.value) // [{id, count}]
      val extraCellCounts = cellCounts.value.filter(x => x._2 < K.value && x._2 > 0)

      // TODO: Get rid of this?
      val fullCellIds = sc.broadcast(HashSet() ++ fullCellCounts.map(x => x._1))
      val extraCellIds = sc.broadcast(HashSet() ++ extraCellCounts.map(x => x._1))


      val fullCells = cells.filter(x => fullCellIds.value.contains(x._1)).persist()
      val extraCells = cells.filter(x => extraCellIds.value.contains(x._1)).persist()


      // At this point we are trained, I think, so now it's a matter of running the training set across all this?

      val testRecords: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(Import.rowOfStr)

      val keyedTestRecords: RDD[(Long, IrisPoint)] = testRecords.keyBy(kNN.pointToCellID)

      // val otherTestingStuff = keyedTestRecords.count()
      // Pass to find inital KNNs, and to calculate point-eqidistant bounding geometry
      val bucketedRecords: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] =
        keyedTestRecords.cogroup(cells) // TODO: Maybe persist here???
      // See if we get an exception here.
      val testingStuff = bucketedRecords.count()

      val full: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] = bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
        val key = cell._1
        cellCounts.value(key) >= K.value
      })

      val full1 = full.mapPartitions(part => {
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

            val dist = (new Vector2D(center.x, center.y) - new Vector2D(farthestPoint.x, farthestPoint.y)).len()
            // Doopy?
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
        Array(1, 2).iterator
      })

      val needsAdditionalData: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))]
        = bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
        val key = cell._1
        // Fishing out the testIter
        val testIter = cell._2._1
        cellCounts.value(key) < K.value && testIter.count(p => true) > 0
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
          var enclosedIdCount = cellCounts.value(cellID)
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

      var qq: RDD[(Long, (Iterable[(IrisPoint)], Iterable[IrisPoint]))] =
        cellIdsAndPidsToAddCheckTo.cogroup(cells);


      // TODO: Come back to this after some ponderation

      /*
      val knnOfUndersuppliedCells /* : RDD[(IrisPoint, Array[IrisPoint])] */ =
        cellIdsAndPidsToAddCheckTo
          .join(cells)
            .flatMap(arg =>{
              arg.
            })
      */

      /*
      val knnOfUndersuppliedCells: RDD[(IrisPoint, Array[IrisPoint])] =
      cellIdsAndPidsToAddCheckTo.keyBy(x => x._1) // Key by the cell_id
          .join(cells) // RDD[(cell_id, Iterable[IrisPoint], Iterable[IrisPoint])]
          .flatMap(arg => {
            // var c_id = overlapped._0;
            val test = arg._2;
            val train = arg._3;
            1
          })
        */
    }

  }

