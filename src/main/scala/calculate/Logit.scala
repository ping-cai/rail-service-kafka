package calculate

import java.util

import distribution.LogitDistribute
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.mutable

object Logit {
  def logit(pathDynamicCost: java.util.Map[Path, Double],
            minPathCost: (Path, Double), passengers: Double): java.util.Map[Path, Double] = {
    val exp = Math.E
    var pathTotalCost = 0.0
    val minCost = minPathCost._2
    val pathWithLogitFlow = new util.HashMap[Path, Double]()
    pathDynamicCost.forEach((path, cost) => {
      pathTotalCost += Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost))
    })
    //    防止除0异常
    if (pathTotalCost == 0.0) {
      throw new RuntimeException(s"Divide by zero exception!check the pathTotalCost why it is zero")
    }
    pathDynamicCost.forEach((path, cost) => {
      val distributionPower = Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost)) / pathTotalCost
      pathWithLogitFlow.put(path, distributionPower * passengers)
    })
    pathWithLogitFlow
  }

  def logit(pathCost: mutable.HashMap[PathWithId, Double],
            minPathCost: (PathWithId, Double), passengers: Double): mutable.HashMap[PathWithId, Double] = {
    val exp = Math.E
    var pathTotalCost = 0.0
    val minCost = minPathCost._2
    val pathWithLogitFlow = new mutable.HashMap[PathWithId, Double]()
    pathCost.foreach(x => {
      val cost = x._2
      pathTotalCost += Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost))
    })
    //    防止除0异常
    if (pathTotalCost == 0.0) {
      throw new RuntimeException(s"Divide by zero exception!check the pathTotalCost why it is zero")
    }
    pathCost.foreach(x => {
      val pathWithId = x._1
      val cost = x._2
      val distributionPower = Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost)) / pathTotalCost
      pathWithLogitFlow.put(pathWithId, distributionPower * passengers)
    })
    pathWithLogitFlow
  }
}
