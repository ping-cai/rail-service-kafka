package costcompute

import java.util

import costcompute.back.BackCompose
import domain.param.CostParam
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StopTimeCost extends CostCompose with BackCompose {
  private var stopTime: Double = CostParam.STOP_STATION_TIME / 60

  def this(stopTime: Double) {
    this()
    this.stopTime = stopTime
  }

  /**
    * 遍历K短路，得到运行的区间数*stopTime即可得到停站旅行时间
    *
    * @param kspResult K短路结果
    * @return 特定的费用
    */
  override def compose(kspResult: util.List[Path]): util.Map[Path, Double] = {
    val pathWithStopTime = new util.HashMap[Path, Double]()
    kspResult.forEach(x => {
      val edges = x.getEdges
      val allEdgesStopTime = edges.size() * stopTime
      pathWithStopTime.put(x, allEdgesStopTime)
    })
    pathWithStopTime
  }

  override def compose(pathBuffer: ListBuffer[PathWithId]): mutable.HashMap[PathWithId, Double] = {
    val pathWithStopTime = mutable.HashMap[PathWithId, Double]()
    pathBuffer.foreach(x => {
      val path = x.path
      val edges = path.getEdges
      val allEdgesStopTime = edges.size() * stopTime
      pathWithStopTime.put(x, allEdgesStopTime)
    })
    pathWithStopTime
  }
}
