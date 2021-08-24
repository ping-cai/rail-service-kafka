package domain.param

trait AdjustParam[T] extends Serializable {
  def adjust(param: T)
}
