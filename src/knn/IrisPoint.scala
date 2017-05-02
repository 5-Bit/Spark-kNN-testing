package knn

class IrisPoint(
  val pid: Long,
  val x: Double,
  val y: Double,
  val w: Double,
  val z: Double,
  val classification: String) extends Serializable {
    override def equals(other: Any) = other match {
      case that: IrisPoint =>
        this.x == that.x && this.y == that.y && this.w == that.w && this.z == that.z && this.pid == that.pid && this.classification == that.classification
      case _ => false
    }

  override def toString: String =
    s"{ pid: $pid, x: $x, y: $y, w: $w, x: $z, classification: $classification }"
}

object IrisPointAccessorsFactoryThisSucks {
  val getX = (i: IrisPoint) => i.x
  val getY = (i: IrisPoint) => i.y
}