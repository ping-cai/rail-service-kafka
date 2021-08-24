package distribution

import java.{lang, util}

import conf.DynamicConf
import dataload.BaseDataLoad
import domain.Section
import domain.param.{AdjustParam, CostParam}
import exception.TravelTimeNotExistException
import kspcalculation.{Edge, Path}
import org.slf4j.{Logger, LoggerFactory}
import utils.{DateExtendUtil, TimeKey}

class StaticLogitDistribute(pathDynamicCost: java.util.Map[Path, Double],
                            minPathCost: (Path, Double),
                            private var distributionResult: DistributionResult,
                            baseDataLoad: BaseDataLoad) extends LogitDistribute with AdjustParam[Double] {
  private var timeInterval: Int = DynamicConf.timeInterval
  private var TRANSFER_TIME = CostParam.TRANSFER_TIME
  private var STOP_STATION_TIME = CostParam.STOP_STATION_TIME

  def this(pathDynamicCost: java.util.Map[Path, Double],
           minPathCost: (Path, Double),
           distributionResult: DistributionResult,
           baseDataLoad: BaseDataLoad, timeInterval: Int) {
    this(pathDynamicCost, minPathCost, distributionResult, baseDataLoad)
    this.timeInterval = timeInterval
  }

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val sectionWithDirectionMap: util.Map[Section, String] = baseDataLoad.getSectionWithDirectionMap

  override def logit(passengers: Double, startTime: String): DistributionResult = {
    val exp = Math.E
    var pathTotalCost = 0.0
    val minCost = minPathCost._2
    val pathWithDistributedFlow = new util.HashMap[Path, Double]()
    pathDynamicCost.forEach((path, cost) => {
      pathTotalCost += Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost))
    })
    //    防止除0异常
    if (pathTotalCost == 0.0) {
      throw new RuntimeException(s"Divide by zero exception!check the pathTotalCost why it is zero")
    }
    pathDynamicCost.forEach((path, cost) => {
      val distributionPower = Math.pow(exp, (-LogitDistribute.theta) * (cost / minCost)) / pathTotalCost
      pathWithDistributedFlow.put(path, distributionPower * passengers)
    })

    def distributeFlow(pathWithFlow: java.util.Map[Path, Double]): DistributionResult = {
      val sectionTraffic = distributionResult.getTimeIntervalTraffic.getTimeSectionTraffic
      val stationFlow = distributionResult.getTimeIntervalStationFlow.getTimeStationTraffic
      val transferFlow = distributionResult.getTimeIntervalTransferFlow.getTimeIntervalTransferFlow
      val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
      val odStartTime = startTime
      pathWithFlow.forEach((path, flow) => {
        var pathTempInTime = odStartTime
        val edges = path.getEdges
        for (edgeNum <- 0 until edges.size()) {
          val edge = edges.get(edgeNum)
          val section = Section.createSectionByEdge(edge)
          val direction = sectionWithDirectionMap.get(section)
          val travelSeconds = sectionTravelGraph.getTravelTime(section)
          //          如果为0是换乘，不为0就不是换乘
          if (direction != null) {
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
              //              这里给的区间一定是同时存在于列车运行时间内的
              updateTimeSectionFlow(sectionTraffic, flow, timeKey, section)
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
            updateTimeTransferFlow(transferFlow, flow, edges, edgeNum, section, timeKey)
            updateTimeStationFlow(stationFlow, flow, edge, timeKey)
            pathTempInTime = tempNextTime
          }
          pathTempInTime = DateExtendUtil.timeAdditionSecond(pathTempInTime, STOP_STATION_TIME.toInt)
        }
      })
      distributionResult
    }

    //如果抛出异常，那么就返回原来的结果
    distributeFlow(pathWithDistributedFlow)
  }

  /**
    *
    * @param stationFlow 某时段的车站流量表
    * @param flow        将要叠加的流量，此流量会叠加到该时段车站流量表上
    * @param edge        边集，用于判断应该叠加的车站是否为换乘站，判断进站和出站
    * @param timeKey     时段Key
    * @return
    */
  private def updateTimeStationFlow(stationFlow: util.Map[TimeKey, util.Map[StationWithType, lang.Double]], flow: Double, edge: Edge, timeKey: TimeKey) = {
    val departure = new StationWithType(edge.getFromNode, "out")
    val arrival = new StationWithType(edge.getToNode, "in")
    //          检查前时段的数据是否包含当前时段
    if (stationFlow.containsKey(timeKey)) {
      val oldTimeStationFlow = stationFlow.get(timeKey)
      //            如果有前时段出站的该车站的客流
      if (oldTimeStationFlow.containsKey(departure)) {
        val oldDepartureFlow = oldTimeStationFlow.get(departure)
        oldTimeStationFlow.put(departure, flow + oldDepartureFlow)
      } else {
        //              如果前时段没有
        oldTimeStationFlow.put(departure, flow)
      }
      //            如果有前时段进站的该车站的客流
      if (oldTimeStationFlow.containsKey(arrival)) {
        val oldArrivalFlow = oldTimeStationFlow.get(arrival)
        oldTimeStationFlow.put(departure, flow + oldArrivalFlow)
      } else {
        //              如果前时段没有
        oldTimeStationFlow.put(departure, flow)
      }
    } else {
      //        如果不包含
      val newTimeStationFlow = new util.HashMap[StationWithType, lang.Double]()
      newTimeStationFlow.put(departure, flow)
      newTimeStationFlow.put(arrival, flow)
      //            添加时段数据，为时段增量
      stationFlow.put(timeKey, newTimeStationFlow)
    }
  }

  /**
    *
    * @param transferFlow 某时段的换乘流量表
    * @param flow         将要叠加的流量
    * @param edges        所有边集的集合
    * @param edgeNum      换乘的边集位置
    * @param section      换乘虚拟区间
    * @param timeKey      时段Key
    * @return
    */
  private def updateTimeTransferFlow(transferFlow: util.Map[TimeKey, util.Map[TransferWithDirection, lang.Double]], flow: Double, edges: util.LinkedList[Edge],
                                     edgeNum: Int, section: Section, timeKey: TimeKey) = {
    //    这里容易指针越界，需要判定是否为首尾换乘的站
    val lastSection = Section.createSectionByEdge(edges.get(edgeNum - 1))
    val nextSection = Section.createSectionByEdge(edges.get(edgeNum + 1))
    val lastDirection = sectionWithDirectionMap.get(lastSection)
    val nextDirection = sectionWithDirectionMap.get(nextSection)
    val transferWithDirection = new TransferWithDirection(section, lastDirection + nextDirection)
    if (transferFlow.containsKey(timeKey)) {
      val oldTransferFlow = transferFlow.get(timeKey)
      //            判断已经出现的时段并且包含的换乘区间是否包含当前时刻的应该加入的换乘区间
      if (oldTransferFlow.containsKey(transferWithDirection)) {
        //              当前换乘区间的换乘人数
        val oldFlow = oldTransferFlow.get(transferWithDirection)
        //              更新人数
        oldTransferFlow.put(transferWithDirection, oldFlow + flow)
      } else {
        //              直接新增
        oldTransferFlow.put(transferWithDirection, flow)
      }
    } else {
      //              没有出现的时段的换乘区间应该初始化新的Map进行添加
      val newTransferFlow: util.Map[TransferWithDirection, lang.Double] = new util.HashMap[TransferWithDirection, lang.Double]()
      newTransferFlow.put(transferWithDirection, flow)
      transferFlow.put(timeKey, newTransferFlow)
    }
  }

  /**
    *
    * @param sectionTraffic 某时段的区间流量表
    * @param flow           将要叠加的流量
    * @param timeKey        时段Key
    * @param section        区间
    * @return
    */
  private def updateTimeSectionFlow(sectionTraffic: util.Map[TimeKey, util.Map[Section, lang.Double]], flow: Double, timeKey: TimeKey, section: Section) = {
    //          判断是否有这个时间段
    if (sectionTraffic.containsKey(timeKey)) {
      val oldTimeSectionFlow = sectionTraffic.get(timeKey)
      //            判断这个时间段是否有这个区间
      if (oldTimeSectionFlow.containsKey(section)) {
        val oldFlow = oldTimeSectionFlow.get(section)
        oldTimeSectionFlow.put(section, oldFlow + flow)
      } else {
        //              这个时间段没有这个区间的流量，对应初始化新的区间流量
        oldTimeSectionFlow.put(section, flow)
      }
    } else {
      //            没有这个时间段，那么初始化这个时间段及其新的区间流量
      val newSectionFlow = new util.HashMap[Section, lang.Double]()
      newSectionFlow.put(section, flow)
      sectionTraffic.put(timeKey, newSectionFlow)
    }
    //最后时间一定要进行叠加更新
  }

  override def adjust(param: Double): Unit = {
    TRANSFER_TIME = param
  }
}
