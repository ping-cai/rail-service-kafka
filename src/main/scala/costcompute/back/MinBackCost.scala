package costcompute.back

import model.back.PathWithId

import scala.collection.mutable

trait MinBackCost extends Serializable{
  def compose(sectionMoveCost:mutable.HashMap[PathWithId, Double]):(PathWithId,Double)
}
