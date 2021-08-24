package costcompute

import java.util

import costcompute.back.MinBackCost
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  *
  */
class MinGeneralizedCost extends MinCost with MinBackCost {
  /**
    *
    * @return 特定的最小费用
    */
  def compose(sectionMoveCost: util.Map[Path, Double]): (Path, Double) = {
    sectionMoveCost.asScala.toList.min((x: (Path, Double), y: (Path, Double)) => {
      (x._2 - y._2).toInt
    })
  }

  override def compose(sectionMoveCost: mutable.HashMap[PathWithId, Double]): (PathWithId, Double) = {
    sectionMoveCost.minBy(x => x._2)
  }
}
