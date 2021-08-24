package costcompute.back

import model.back.PathWithId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait BackCompose extends Serializable {
  def compose(pathBuffer: ListBuffer[PathWithId]): mutable.HashMap[PathWithId, Double]
}
