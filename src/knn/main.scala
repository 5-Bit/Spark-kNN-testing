package knn;
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, HashSet}

  object EntryPoint {

    def main(args: Array[String]): Unit = {

      val sConf = new SparkConf().setAppName("Spark").setMaster("local[*]") // Init spark context
      val sc = new SparkContext(sConf) // Init spark context

      val K = sc.broadcast(5) // Or something from the command line. :/
      val DIM_CELLS = sc.broadcast(kNN.DIM_CELLS) // The number of cells in each dimension

      // TODO: Build import code
      val irisData: RDD[IrisPoint] = sc.textFile("data/iris_train_pid.csv").map(x => Import.rowOfStr(x))

      // This is here because Spark is yo-mama, who is an id-10-t, so bug off
      val x_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).max())
      val y_max: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).max())
      val x_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.x).min())
      val y_min: Broadcast[Double] = sc.broadcast(irisData.map(ir => ir.y).min())

      kNN.xMax = x_max.value
      kNN.yMax = y_max.value
      kNN.xMin = x_min.value
      kNN.yMin = y_min.value

      // pid, x, y, class

      // Assume normalized data, that will be done at some point :D
      // TODO: Build custom partitioner here?
      val cells = irisData.keyBy(kNN.pointToCellID).persist()

      val cellCounts: Broadcast[Map[Long, Long]] = sc.broadcast(cells.countByKey())
      overlapping.cellCounts = cellCounts

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

      val otherTestingStuff = keyedTestRecords.count()
      // Pass to find inital KNNs, and to calculate point-eqidistant bounding geometry
      val bucketedRecords: RDD[(Long, (Iterable[IrisPoint], Iterable[IrisPoint]))] = keyedTestRecords.cogroup(cells).persist()
      // See if we get an exception here.
      val testingStuff = bucketedRecords.count()

      val full = bucketedRecords.filter(cell => { // (key, (testIter, trainIter))
      val key = cell._1
        cellCounts.value(key) >= K.value
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

