package distribution

import java.util

import dataload.BaseDataLoad
import domain.Section
import domain.param.{AdjustParam, CostParam}
import exception.TravelTimeNotExistException
import flowdistribute.OdWithTime
import kspcalculation.{Path, PathService}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import utils.{DateExtendUtil, TimeKey}

class BaseDistribute extends AdjustParam[Double] {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var TRANSFER_TIME = CostParam.TRANSFER_TIME
  private var STOP_STATION_TIME = CostParam.STOP_STATION_TIME

  def distribute(logitResult: util.Map[Path, Double],
                 sectionTravelGraph: util.Map[Section, Int],
                 sectionWithDirectionMap: util.Map[Section, String],
                 startTime: String,
                 timeInterval: Int,
                 dynamic: DynamicResult): DynamicResult = {
    val distributionResult = dynamic.getDistributionResult
    val sectionTraffic = distributionResult.getTimeIntervalTraffic.getTimeSectionTraffic
    val stationFlow = distributionResult.getTimeIntervalStationFlow.getTimeStationTraffic
    val transferFlow = distributionResult.getTimeIntervalTransferFlow.getTimeIntervalTransferFlow
    val odStartTime = startTime
    logitResult.forEach((path, flow) => {
      var pathTempInTime = odStartTime
      val edges = path.getEdges
      for (edgeNum <- 0 until edges.size()) {
        val edge = edges.get(edgeNum)
        val section = Section.createSectionByEdge(edge)
        val direction = sectionWithDirectionMap.get(section)
        val travelSeconds = sectionTravelGraph.get(section)
        //          如果为0是换乘，不为0就不是换乘
        if (!"0".equals(direction)) {
          /*
                  如果不为空，那么就是区间，如果为空，那么就应该是换乘
                  否则就是数据出现问题，找不到路网图的运行时间
                  此处是一个错误还是应该抛出一个异常
                   */
          if (travelSeconds == null) {
            log.warn(s"${TravelTimeNotExistException.MESSAGE} $section")
          } else {
            val tempNextTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, travelSeconds)
            val timeKeyStartTime = TimeKey.timeAddAndBetweenInterval(pathTempInTime, travelSeconds / 2, timeInterval)
            val timeKeyEndTime = TimeKey.startTimeAddToEnd(timeKeyStartTime, timeInterval)
            val timeKey = new TimeKey(timeKeyStartTime, timeKeyEndTime)
            Distribute.updateTimeSectionFlow(sectionTraffic, flow, timeKey, section)
            pathTempInTime = tempNextTime
          }
        }
        //            如果是换乘
        else {
          val transferSeconds = TRANSFER_TIME.toInt
          val tempNextTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, transferSeconds)
          val timeKeyStartTime = TimeKey.timeAddAndBetweenInterval(pathTempInTime, transferSeconds / 2, timeInterval)
          val timeKeyEndTime = TimeKey.startTimeAddToEnd(timeKeyStartTime, timeInterval)
          val timeKey = new TimeKey(timeKeyStartTime, timeKeyEndTime)
          Distribute.updateTimeTransferFlow(transferFlow, flow, edges, edgeNum, section, timeKey, sectionWithDirectionMap)
          val inStation = new StationWithType(edge.getFromNode, "in")
          Distribute.updateTimeStationFlow(stationFlow, flow, inStation, timeKey)
          val outStation = new StationWithType(edge.getToNode, "out")
          Distribute.updateTimeStationFlow(stationFlow, flow, outStation, timeKey)
          pathTempInTime = tempNextTime
        }
        pathTempInTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, STOP_STATION_TIME.toInt)
      }
    })
    dynamic
  }

  override def adjust(param: Double): Unit = {
    TRANSFER_TIME = param
  }
}

object BaseDistribute {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val baseDataLoad = new BaseDataLoad
    val graph = baseDataLoad.getGraph
    val sectionWithDirectionMap = baseDataLoad.getSectionWithDirectionMap
    val pathService = new PathService(3, graph, sectionWithDirectionMap)
    val odWithTime = new OdWithTime("100", "101", "2021-02-24 15:00:00", 3.0)
    val legalPath = pathService.getLegalKPath(odWithTime.getInId, odWithTime.getOutId)
  }
}
