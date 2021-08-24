package costcompute

import java.util

import costcompute.back.BackCompose
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SectionMoveCost(sectionMoveGraph: SectionTravelGraph) extends CostCompose with BackCompose {
  /**
    *
    * @param kspResult K短路结果
    * @return 特定的费用
    *         待解决的问题：列车运行时间也应该是中位数
    */
  override def compose(kspResult: util.List[Path]): java.util.Map[Path, Double] = {
    val sectionMoveCost = new java.util.HashMap[Path, Double]
    kspResult.forEach(
      x => {
        sectionMoveCost.put(x, x.getTotalCost)
      })
    sectionMoveCost
  }

  override def compose(pathBuffer: ListBuffer[PathWithId]): mutable.HashMap[PathWithId, Double] = {
    val sectionMoveCost = mutable.HashMap[PathWithId, Double]()
    pathBuffer.foreach(pathWithId => {
      sectionMoveCost.put(pathWithId, pathWithId.path.getTotalCost)
    })
    sectionMoveCost
  }
}
