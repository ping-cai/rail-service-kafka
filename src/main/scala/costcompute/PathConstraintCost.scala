package costcompute

import java.util

import conf.DynamicConf
import costcompute.back.{BackCompose, PathWithIdTransfer}
import domain.Section
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PathConstraintCost(sectionWithDirectionMap: util.Map[Section, String]) extends CostCompose with BackCompose {
  //    换乘次数偏离约束
  val transferTimesDeviation: Int = DynamicConf.transferTimesDeviation
  //    出行费用放大系数约束
  val travelExpenseAmplification: Double = DynamicConf.travelExpenseAmplification
  //    出行费用最大值约束
  val maximumTravelCost: Double = DynamicConf.maximumTravelCost

  /**
    *
    * @param kspResult K短路结果
    * @return 特定的费用
    */
  override def compose(kspResult: util.List[Path]): util.Map[Path, Double] = {
    val pathWithTransferAndCosts = kspResult.asScala.map(path => {
      var tempTransferTime = 0
      path.getEdges.forEach(edge => {
        //        如果是换乘
        if (null == sectionWithDirectionMap.get(Section.createSectionByEdge(edge))) {
          tempTransferTime += 1
        }
      })
      //      路径总费用
      val totalCost = path.getTotalCost
      //      存储路径，换乘次数，总费用
      PathWithTransferAndCost(path, tempTransferTime, totalCost)
    })
    val minCost = pathWithTransferAndCosts.minBy(x => x.cost).cost
    val minTransferTime = pathWithTransferAndCosts.minBy(x => x.transferTime).transferTime
    val result = new util.HashMap[Path, Double]()
    pathWithTransferAndCosts foreach (x => {
      if (x.cost <= ((1 + travelExpenseAmplification) * minCost) &&
        x.cost <= (minCost + maximumTravelCost) &&
        x.transferTime <= (minTransferTime + transferTimesDeviation)) {
        result.put(x.path, x.cost)
      }
    })
    result
  }

  override def compose(pathBuffer: ListBuffer[PathWithId]): mutable.HashMap[PathWithId, Double] = {
    val pathWithInAndTransfer = pathBuffer.map(pathWithId => {
      val path = pathWithId.path
      var tempTransferTime = 0
      path.getEdges.forEach(edge => {
        //        如果是换乘
        if (null == sectionWithDirectionMap.get(Section.createSectionByEdge(edge))) {
          tempTransferTime += 1
        }
      })
      //      存储路径，换乘次数，总费用
      PathWithIdTransfer(pathWithId, tempTransferTime)
    })
    val minCost = pathWithInAndTransfer.minBy(x => x.pathWithId.path.getTotalCost).pathWithId.path.getTotalCost
    val minTransferTime = pathWithInAndTransfer.minBy(x => x.transferTime).transferTime
    val result = mutable.HashMap[PathWithId, Double]()
    pathWithInAndTransfer foreach (x => {
      val cost = x.pathWithId.path.getTotalCost
      if (cost <= ((1 + travelExpenseAmplification) * minCost) &&
        cost <= (minCost + maximumTravelCost) &&
        x.transferTime <= (minTransferTime + transferTimesDeviation)) {
        result.put(x.pathWithId, x.pathWithId.path.getTotalCost)
      }
    })
    result
  }
}
