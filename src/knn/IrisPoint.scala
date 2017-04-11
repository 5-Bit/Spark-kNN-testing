package knn
class IrisPoint(val pid: Long, val x:Double, val y:Double, val w: Double, val z:Double, val classification: String) extends Serializable {

}

object IrisPointAccessorsFactoryThisSucks {
  val getX = (i: IrisPoint) => i.x
  val getY = (i: IrisPoint) => i.y
}