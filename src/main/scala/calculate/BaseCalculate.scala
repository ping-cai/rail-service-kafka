package calculate

import java.{lang, util}

import conf.DynamicConf
import costcompute._
import dataload.BaseDataLoad
import distribution._
import domain.Section
import domain.param.{AdjustParam, CostParam}
import exception.TravelTimeNotExistException
import flowdistribute.OdWithTime
import kspcalculation.{Graph, Path, PathService}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import utils.{DateExtendUtil, TimeKey}

/**
  * 首先加载和计算需要异步
  * 加载所有数据，计算就只需要外部输入值
  * 此处聚合所有的计算逻辑，输入值
  * 这里基本逻辑先理通
  *
  * @param baseDataLoad       所有基本数据加载
  * @param sectionTravelGraph 区间运行时间
  */
class BaseCalculate(baseDataLoad: BaseDataLoad, sectionTravelGraph: SectionTravelGraph) extends AdjustParam[Double] {
  private val sectionWithDirectionMap: util.Map[Section, String] = baseDataLoad.getSectionWithDirectionMap
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var TRANSFER_TIME = CostParam.TRANSFER_TIME
  private var STOP_STATION_TIME = CostParam.STOP_STATION_TIME

  def getLegalPathList(kspNum: Int, odWithTime: OdWithTime): java.util.List[Path] = {
    val graph: Graph = baseDataLoad.getGraph
    val pathComputeService = new PathService(kspNum, graph, sectionWithDirectionMap)
    //    一定要去除首尾有换乘的站点,这样的路径才是合法路径
    val legalPath = pathComputeService.getLegalKPath(odWithTime.getInId, odWithTime.getOutId)
    legalPath
  }

  //    列车停站费用 +  区间运行费用 =静态费用，这些都是固定的费用
  def getStaticCost(legalPath: util.List[Path]): util.Map[Path, Double] = {
    val staticCost: util.Map[Path, Double] = new StaticCost(baseDataLoad.getSectionWithDirectionMap).costCompute(legalPath, sectionTravelGraph)
    //    额外添加区间阻塞费用加以引导，插件功能
    if (DynamicConf.openSectionAdjust) {
      val sectionHashMap = baseDataLoad.getSectionExtraMap
      staticCost.forEach((path, cost) => {
        var extraCost = 0.0
        path.getEdges.forEach(edge => {
          val section = Section.createSectionByEdge(edge)
          if (sectionHashMap contains section) {
            extraCost += sectionHashMap(section)
          }
        })
        staticCost.put(path, cost + extraCost)
      })
    }
    staticCost
  }

  def dynamicCalculate(odWithTime: OdWithTime, staticCost: util.Map[Path, Double], minGeneralizedCost: (Path, Double), legalPath: util.List[Path],
                       timeIntervalTraffic: TimeIntervalTraffic, interval: Int): util.Map[Path, Double] = {
    //    获取列车通行区间数据
    val trainOperationSection = baseDataLoad.getTrainOperationSection
    //    列车拥挤费用
    val trainCrowdCost = new TrainCrowdCost(sectionTravelGraph, timeIntervalTraffic, trainOperationSection, odWithTime.getInTime, interval)
    //    val trainCrowdPathCost = trainCrowdCost.compose(legalPath)
    val dynamicCost = new DynamicCost(trainCrowdCost, staticCost).costCompute(legalPath, sectionTravelGraph)
    val logitResult = Logit.logit(dynamicCost, minGeneralizedCost, odWithTime.getPassengers / 3)
    logitResult
  }

  def distribute(logitResult: util.Map[Path, Double],
                 startTime: String,
                 timeInterval: Int,
                 result: DistributionResult, temp: DistributionResult): DistributionWithTemp = {
    val sectionTraffic = result.getTimeIntervalTraffic.getTimeSectionTraffic
    val stationFlow = result.getTimeIntervalStationFlow.getTimeStationTraffic
    val transferFlow = result.getTimeIntervalTransferFlow.getTimeIntervalTransferFlow
    //    某个OD分配的暂时结果
    val tempSectionTraffic = temp.getTimeIntervalTraffic.getTimeSectionTraffic
    val tempStationFlow = temp.getTimeIntervalStationFlow.getTimeStationTraffic
    val tempTransferFlow = temp.getTimeIntervalTransferFlow.getTimeIntervalTransferFlow
    val odStartTime = startTime
    logitResult.forEach((path, flow) => {
      var pathTempInTime = odStartTime
      val edges = path.getEdges
      val size = edges.size() - 1
      //      log.error(s"3 this sectionTravelGraph is ${sectionTravelGraph.sectionTravelMap}")
      for (edgeNum <- 0 to size) {
        val edge = edges.get(edgeNum)
        val section = Section.createSectionByEdge(edge)
        val direction = sectionWithDirectionMap.get(section)
        val travelSeconds = sectionTravelGraph.getTravelTime(section)
        //          如果为空是换乘，不为空就不是换乘
        if (null != direction) {
          /*
                  如果不为空，那么就是区间，如果为空，那么就应该是换乘
                  否则就是数据出现问题，找不到路网图的运行时间
                  此处是一个错误还是应该抛出一个异常
                   */
          if (travelSeconds == null) {
            log.error(s"${TravelTimeNotExistException.MESSAGE} $section")
          } else {
            val tempNextTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, travelSeconds)
            //            val timeKeyStartTime = TimeKey.timeAddAndBetweenInterval(pathTempInTime, travelSeconds / 2, timeInterval)
            //            val timeKeyEndTime = TimeKey.startTimeAddToEnd(timeKeyStartTime, timeInterval)
            //            val timeKey = new TimeKey(timeKeyStartTime, timeKeyEndTime)
            val timeKey = TimeKey.getTimeKeyOfOneTimeAdd(pathTempInTime, travelSeconds / 2, timeInterval)
            //            最终的结果集更新
            Distribute.updateTimeSectionFlow(sectionTraffic, flow, timeKey, section)
            //            暂时的结果集更新
            Distribute.updateTimeSectionFlow(tempSectionTraffic, flow, timeKey, section)
            pathTempInTime = tempNextTime
            //            log.error(s"2 at this section_result $sectionTraffic")
          }
        }
        //            如果是换乘
        else {
          val transferSeconds = TRANSFER_TIME.toInt
          val tempNextTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, transferSeconds)
          val timeKey = TimeKey.getTimeKeyOfOneTimeAdd(pathTempInTime, transferSeconds / 2, timeInterval)
          //          最终的结果集更新
          Distribute.updateTimeTransferFlow(transferFlow, flow, edges, edgeNum, section, timeKey, sectionWithDirectionMap)
          /*
          暂时的结果集更新
           */
          Distribute.updateTimeTransferFlow(tempTransferFlow, flow, edges, edgeNum, section, timeKey, sectionWithDirectionMap)
          val inStation = new StationWithType(edge.getFromNode, "in")
          //          最终的结果集更新
          Distribute.updateTimeStationFlow(stationFlow, flow, inStation, timeKey)
          val outStation = new StationWithType(edge.getToNode, "out")
          //          最终的结果集更新
          Distribute.updateTimeStationFlow(stationFlow, flow, outStation, timeKey)
          /*
          暂时的结果集更新
           */
          Distribute.updateTimeStationFlow(tempStationFlow, flow, inStation, timeKey)
          Distribute.updateTimeStationFlow(tempStationFlow, flow, outStation, timeKey)
          pathTempInTime = tempNextTime
        }
        pathTempInTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, STOP_STATION_TIME.toInt)
      }
      val firstTimeKey = TimeKey.getTimeKeyOfOneTimeAdd(odStartTime, 0, timeInterval)
      val inStation = new StationWithType(path.getEdges.getFirst.getFromNode, "in")
      //          最终的结果集更新
      Distribute.updateTimeStationFlow(stationFlow, flow, inStation, firstTimeKey)
      val lastTimeKey = TimeKey.getTimeKeyOfOneTimeAdd(pathTempInTime, 0, timeInterval)
      val outStation = new StationWithType(path.getEdges.getLast.getToNode, "out")
      //          最终的结果集更新
      Distribute.updateTimeStationFlow(stationFlow, flow, outStation, lastTimeKey)
      /*
          暂时的结果集更新
           */
      Distribute.updateTimeStationFlow(tempStationFlow, flow, inStation, firstTimeKey)
      Distribute.updateTimeStationFlow(tempStationFlow, flow, outStation, lastTimeKey)
    })
    new DistributionWithTemp(result, temp)
  }

  override def adjust(param: Double): Unit = {
    TRANSFER_TIME = param
  }
}

object BaseCalculate {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val baseDataLoad = new BaseDataLoad
    val timeSectionTraffic = new util.HashMap[TimeKey, util.Map[Section, java.lang.Double]]()
    val intervalTraffic = new TimeIntervalTraffic(timeSectionTraffic)
    val odWithTime = new OdWithTime("50", "111", "2021-05-04 17:15:00", 6000.0)
    val interval = 15
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    val baseCalculate = new BaseCalculate(baseDataLoad, sectionTravelGraph)
    val legalPath = baseCalculate.getLegalPathList(5, odWithTime: OdWithTime)
    val staticCost = baseCalculate.getStaticCost(legalPath)
    val minGeneralizedCost = new MinGeneralizedCost().compose(staticCost)
    val timeSectionFlow = new util.HashMap[TimeKey, util.Map[Section, lang.Double]]()
    val timeIntervalTraffic = new TimeIntervalTraffic(timeSectionFlow)
    val timeStationFlow = new util.HashMap[TimeKey, util.Map[StationWithType, lang.Double]]()
    val timeIntervalStationFlow = new TimeIntervalStationFlow(timeStationFlow)
    val timeTransferFlow = new util.HashMap[TimeKey, util.Map[TransferWithDirection, lang.Double]]()
    val timeIntervalTransferFlow = new TimeIntervalTransferFlow(timeTransferFlow)
    val distributionResult = new DistributionResult(timeIntervalTraffic, timeIntervalStationFlow, timeIntervalTransferFlow)

  }
}