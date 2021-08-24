package costcompute

import java.util

import conf.DynamicConf
import costcompute.back.BackCost
import domain.Section
import kspcalculation.Path
import model.back.PathWithId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class StaticCost(sectionWithDirectionMap: util.Map[Section, String]) extends Cost with BackCost {
  def costCompute(legalPath: java.util.List[Path], sectionTravelGraph: SectionTravelGraph): java.util.Map[Path, Double] = {
    var sectionMovePathCost: util.Map[Path, Double] = new util.HashMap[Path, Double]()
    if (DynamicConf.openPathConstraint) {
      val pathConstraintCost = new PathConstraintCost(sectionWithDirectionMap)
      sectionMovePathCost = pathConstraintCost.compose(legalPath)
    } else {
      //    区间运行费用
      val sectionMoveCost = new SectionMoveCost(sectionTravelGraph)
      sectionMovePathCost = sectionMoveCost.compose(legalPath)
    }
    //    列车停站费用
    val stopTimeCost = new StopTimeCost()
    val stopTimePathCost = stopTimeCost.compose(legalPath)
    /*
      * 三者聚合，应当创建一个新的集合对象进行存储
      * 否则将改变原有三者集合中的数据，破坏其原有的语义
      */
    val allPathCost = new util.HashMap[Path, Double]()
    //    随机做某个集合的循环即可，因为元素都是一致的
    sectionMovePathCost.forEach((path, cost) => {
      val allCost = cost + stopTimePathCost.get(path)
      allPathCost.put(path, allCost)
    })
    sectionMovePathCost
  }

  override def costCompute(pathBuffer: ListBuffer[PathWithId], sectionTravelGraph: SectionTravelGraph, sectionExtraMap: mutable.HashMap[Section, Double]): mutable.HashMap[PathWithId, Double] = {
    //    区间运行费用
    val pathConstraintCost = new PathConstraintCost(sectionWithDirectionMap)
    val sectionMoveCost = pathConstraintCost.compose(pathBuffer)
    //    列车停站费用
    val stopTimeCost = new StopTimeCost()
    val stopTimePathCost = stopTimeCost.compose(pathBuffer)
    /*
      * 三者聚合，应当创建一个新的集合对象进行存储
      * 否则将改变原有三者集合中的数据，破坏其原有的语义
      */
    val allPathCost = mutable.HashMap[PathWithId, Double]()
    //    随机做某个集合的循环即可，因为元素都是一致的
    sectionMoveCost.foreach(x => {
      val pathWithId = x._1
      val cost = pathWithId.path.getTotalCost
      val allCost = cost + stopTimePathCost(pathWithId)
      allPathCost.put(pathWithId, allCost)
    })
    //    额外添加区间阻塞费用加以引导，插件功能
    if (DynamicConf.openSectionAdjust) {
      allPathCost.foreach(x => {
        val pathWithId = x._1
        val path = pathWithId.path
        val cost = x._2
        var extraCost = 0.0
        path.getEdges.forEach(edge => {
          val section = Section.createSectionByEdge(edge)
          if (sectionExtraMap contains section) {
            extraCost += sectionExtraMap(section)
          }
        })
        allPathCost.put(pathWithId, cost + extraCost)
      })
    }
    allPathCost
  }
}
