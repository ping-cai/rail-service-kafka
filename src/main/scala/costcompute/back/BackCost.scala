package costcompute.back

import costcompute.SectionTravelGraph
import domain.Section
import model.back.PathWithId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait BackCost extends Serializable {
  def costCompute(pathBuffer: ListBuffer[PathWithId], sectionTravelGraph: SectionTravelGraph, sectionExtraMap: mutable.HashMap[Section, Double]): mutable.HashMap[PathWithId, Double]
}
