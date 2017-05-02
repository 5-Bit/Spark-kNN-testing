package knn

class Vector2D(val x: Double, val y: Double) {

  def +(other: Vector2D): Vector2D ={
    new Vector2D(other.x + x, other.y + y)
  }

  def -(other: Vector2D): Vector2D ={
    new Vector2D(other.x - x, other.y - y)
  }

  def *(other: Vector2D): Double ={
    (x * other.x) + (y * other.y)
  }

  def scalar_mul(other: Double): Vector2D ={
    new Vector2D(x * other, y * other)
  }

  def len(): Double ={
    math.sqrt(this * this)
  }

  def unit(): Vector2D ={
    val length = len()
    new Vector2D(x / length, y / length)
  }

  def ==(other: Vector2D): Boolean= {
    x == other.x && y == other.y
  }

}