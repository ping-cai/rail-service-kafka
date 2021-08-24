package calculate

import java.{lang, util}

import costcompute._
import dataload.BaseDataLoad
import distribution.{DistributionResult, DynamicLogitDistribute, StationWithType, TransferWithDirection}
import domain.Section
import flowdistribute.OdWithTime
import kspcalculation.{Graph, Path, PathService}
import utils.TimeKey

class DynamicCalculate(baseDataLoad: BaseDataLoad, odWithTime: OdWithTime) {
  private val sectionWithDirectionMap: util.Map[Section, String] = baseDataLoad.getSectionWithDirectionMap

  def getPathRemovedList(kspNum: Int): java.util.List[Path] = {
    val graph: Graph = baseDataLoad.getGraph
    val pathComputeService = new PathService(kspNum, graph, sectionWithDirectionMap)
    //    一定要去除首尾有换乘的站点,这样的路径才是合法路径
    val legalPath = pathComputeService.getLegalKPath(odWithTime.getInId, odWithTime.getOutId)
    legalPath
  }

  def calculate(legalPath: java.util.List[Path], timeIntervalTraffic: TimeIntervalTraffic, interval: Int): DistributionResult = {
    //    区间运行图的创建
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    //    //    区间运行费用
    //    val sectionMoveCost = new SectionMoveCost(sectionTravelGraph)
    //    val sectionMovePathCost = sectionMoveCost.compose(legalPath)
    //    //    列车停站费用
    //    val stopTimeCost = new StopTimeCost()
    //    val stopTimePathCost = stopTimeCost.compose(legalPath)
    //    我们将费用进行整合，依此得到静态费用的计算
    val staticCost = new StaticCost(baseDataLoad.getSectionWithDirectionMap)
    val staticPathCost = staticCost.costCompute(legalPath, sectionTravelGraph)
    //    获取列车通行区间数据
    val trainOperationSection = baseDataLoad.getTrainOperationSection
    //    列车拥挤费用
    val trainCrowdCost = new TrainCrowdCost(sectionTravelGraph, timeIntervalTraffic, trainOperationSection, odWithTime.getInTime, interval)
    //    val trainCrowdPathCost = trainCrowdCost.compose(legalPath)
    val dynamicCost = new DynamicCost(trainCrowdCost, staticPathCost)
    val dynamicPathCost = dynamicCost.costCompute(legalPath, sectionTravelGraph)
    //    sectionMovePathCost.forEach((path, cost) =>
    //      println(s"路径是$path 最初的费用是$cost"))
    //最小广义费用
    val minGeneralizedCost = new MinGeneralizedCost().compose(staticPathCost)
    //    allPathCost.forEach((path, cost) =>
    //      println(s"路径是$path 总费用是$cost")
    //    )
    val timeIntervalStationMap = new util.HashMap[TimeKey, util.Map[StationWithType, java.lang.Double]]()
    val timeIntervalStationFlow = new TimeIntervalStationFlow(timeIntervalStationMap)
    val timeIntervalTransferMap = new util.HashMap[TimeKey, util.Map[TransferWithDirection, lang.Double]]()
    val timeIntervalTransferFlow = new TimeIntervalTransferFlow(timeIntervalTransferMap)
    val result = new DistributionResult(timeIntervalTraffic, timeIntervalStationFlow, timeIntervalTransferFlow)
    val dynamicDistribute = new DynamicLogitDistribute(dynamicPathCost, minGeneralizedCost, result, sectionTravelGraph, sectionWithDirectionMap)
    val distributionResult = dynamicDistribute.logit(odWithTime.getPassengers, odWithTime.getInTime)
    //    distributionResult.getTimeIntervalTraffic.getTimeSectionTraffic.forEach((timeKey, sectionMap) => {
    //      sectionMap.forEach((section, flow) => {
    //        println(s"时间为$timeKey 区间为$section 流量为$flow")
    //      })
    //    })
    distributionResult
  }
}
