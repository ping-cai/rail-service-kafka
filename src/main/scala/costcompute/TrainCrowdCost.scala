package costcompute

import java.{lang, util}

import costcompute.back.BackCompose
import domain.Section
import domain.param.{AdjustParam, CostParam}
import exception.SectionNotExistException
import kspcalculation.Path
import model.back.PathWithId
import org.slf4j.{Logger, LoggerFactory}
import tools.StationTimeHandle
import utils.{DateExtendUtil, TimeKey}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  *
  * @param sectionTravelGraph    区间运行图，提供区间及其运行时间List集合
  * @param timeIntervalTraffic   路网图的时段流量
  * @param trainOperationSection 区间->列车 Map集合
  * @param startTime             OD的进站时间
  * @param interval              时间间隔粒度
  *                              待修改的东西有：区间运行时间，取中位数
  */
class TrainCrowdCost(sectionTravelGraph: SectionTravelGraph, var timeIntervalTraffic: TimeIntervalTraffic, trainOperationSection: TrainOperationSection,
                     startTime: String, interval: Int) extends CostCompose with AdjustParam[Double] with BackCompose {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var TRANSFER_TIME = CostParam.TRANSFER_TIME
  private var STOP_STATION_TIME = CostParam.STOP_STATION_TIME
  private var A = CostParam.A
  private var B = CostParam.B

  /**
    * 1.区间在这个时段通过了哪些列车，区间人数/列车数=单个列车承载人数，列车数以到达时间为准
    * 2.列车拥挤度理论公式三个阶段，<小于列车座位数><小于列车最大载客数但大于座位数><大于列车最大载客数>
    * 3.根据公示计算每条路径的拥挤费用即可，还有α，β参数
    * 4.路径根据区间运行表得到 <区间,时间段> 和列车通过数
    * 5.再通过Map<时间段,Map<区间表,人数>>和区间通过的某条线路，某条线路得到某种列车，计算列车拥挤度
    *
    * @param kspResult K短路结果
    * @return 特定的费用
    */
  override def compose(kspResult: util.List[Path]): util.Map[Path, Double] = {
    val sectionListMap = sectionTravelGraph.getSectionListMap
    val timeSectionTraffic = timeIntervalTraffic.getTimeSectionTraffic
    val sectionTrainMap = trainOperationSection.getSectionTrainMap
    val pathWithTrainCrowdCost = new util.HashMap[Path, Double]
    val sectionTravelMap = sectionTravelGraph.sectionTravelMap
    kspResult.forEach(
      path => {
        //       注意：每条路径开始时的区间时间应该重置
        var tempTime = startTime
        //        区间边集集合，区间的一种表示
        val edges = path.getEdges
        //      设置路径费用，方便后续叠加
        var pathCost = 0.0
        //        遍历该边集
        edges.forEach(edge => {
          //          通过边集得到区间对象
          val section = Section.createSectionByEdge(edge)
          //          通过区间对象获得区间运行时间列表集
          val travelTimes: util.List[TravelTime] = sectionListMap.get(section)
          //          因为如果出现换乘，那么在区间运行时间集和里就查不到数据，故需要一个判断是否为空操作
          if (travelTimes != null) {
            //            获取比较准确的运行时间，即区间所有列车运行时间的中位数
            val seconds = sectionTravelMap.get(section)
            //            println(s"运行的时间通常有 $seconds 秒")
            //            第一个时间+刚才得到的秒数据，即得到一个区间的开始时间tempTime和结束时间nextTime
            val nextTime = DateExtendUtil.timeAdditionSecond(tempTime, seconds / 2)
            //            得到每个区间对应的时段对象
            val timeKey = StationTimeHandle.getSpecificTimePeriodOfTwoTimePoints(tempTime, nextTime, interval)
            //            得到该时段的区间流量Map集，可能会产生空指针
            val sectionPassengersMap = timeSectionTraffic.get(timeKey)
            //            通过区间确定客流人数

            val passengersTry = Try(sectionPassengersMap.get(section))
            var passengers = 0.0
            if (passengersTry.isFailure) {
              log.warn("exception occurred! {} Our approach is putting the timeKey as key and sectionPassengersMap as value to timeSectionTraffic", SectionNotExistException.MESSAGE)
              val newSectionPassengersMap = new util.HashMap[Section, lang.Double]()
              newSectionPassengersMap.put(section, passengers)
              timeSectionTraffic.put(timeKey, newSectionPassengersMap)
            } else {
              passengers = passengersTry.get
            }
            //            通过列车运行的到达时间和时段确定该时段通过的列车数
            val trainNumber = getTrainNum(timeKey, travelTimes)
            //            通过区间和列车运行所在区间Map集得到哪种类型的列车
            val train = sectionTrainMap.get(section)
            /*
                        以下就是正式进入费用计算的阶段
                         */
            var trainCost = 0.0
            if (trainNumber != 0) {
              trainCost = trainCostCompute(passengers / trainNumber, train)
            }
            pathCost += trainCost
            //            区间时间递增
            tempTime = DateExtendUtil.timeAdditionSecond(tempTime, seconds)
          }
          else {
            //         增加换乘时间，这里可能会有变动
            tempTime = DateExtendUtil.timeAdditionSecond(tempTime, TRANSFER_TIME.toInt)
          }
          //          每个区间都需要增加停车时间
          tempTime = DateExtendUtil.timeAdditionSecond(tempTime, STOP_STATION_TIME.toInt)
          pathWithTrainCrowdCost.put(path, pathCost)
          //          println(s"通过一次区间后的时间变为了$tempTime")
        })
      }
    )
    pathWithTrainCrowdCost
  }

  def getTrainNum(timeKey: TimeKey, travelTimeList: util.List[TravelTime]): Int = {
    var count = 0
    travelTimeList.forEach(travelTime => {
      if (StationTimeHandle.containsArrivalTime(timeKey, travelTime.getArrivalTime))
        count += 1
    })
    count
  }

  def trainCostCompute(passengers: Double, train: Train): Double = {
    val seats = train.getSeats
    val maxCapacity = train.getMaxCapacity
    var result = 0.0
    if (passengers > seats && passengers <= maxCapacity) {
      result = (A * (passengers - seats)) / seats
    } else if (passengers > maxCapacity) {
      result = (A * (passengers - seats)) / seats + (B * (passengers - maxCapacity)) / maxCapacity
    }
    result
  }


  override def adjust(param: Double): Unit = {
    TRANSFER_TIME = param
  }

  override def compose(pathBuffer: ListBuffer[PathWithId]): mutable.HashMap[PathWithId, Double] = {
    val sectionListMap = sectionTravelGraph.getSectionListMap
    val timeSectionTraffic = timeIntervalTraffic.getTimeSectionTraffic
    val sectionTrainMap = trainOperationSection.getSectionTrainMap
    val pathWithTrainCrowdCost = mutable.HashMap[PathWithId, Double]()
    val sectionTravelMap = sectionTravelGraph.sectionTravelMap
    pathBuffer.foreach(
      pathWithId => {
        val path = pathWithId.path
        //       注意：每条路径开始时的区间时间应该重置
        var tempTime = startTime
        //        区间边集集合，区间的一种表示
        val edges = path.getEdges
        //      设置路径费用，方便后续叠加
        var pathCost = 0.0
        //        遍历该边集
        edges.forEach(edge => {
          //          通过边集得到区间对象
          val section = Section.createSectionByEdge(edge)
          //          通过区间对象获得区间运行时间列表集
          val travelTimes: util.List[TravelTime] = sectionListMap.get(section)
          //          因为如果出现换乘，那么在区间运行时间集和里就查不到数据，故需要一个判断是否为空操作
          if (travelTimes != null) {
            //            获取比较准确的运行时间，即区间所有列车运行时间的中位数
            val seconds = sectionTravelMap.get(section)
            //            println(s"运行的时间通常有 $seconds 秒")
            //            第一个时间+刚才得到的秒数据，即得到一个区间的开始时间tempTime和结束时间nextTime
            val nextTime = DateExtendUtil.timeAdditionSecond(tempTime, seconds / 2)
            //            得到每个区间对应的时段对象
            val timeKey = StationTimeHandle.getSpecificTimePeriodOfTwoTimePoints(tempTime, nextTime, interval)
            //            得到该时段的区间流量Map集，可能会产生空指针
            val sectionPassengersMap = timeSectionTraffic.get(timeKey)
            //            通过区间确定客流人数

            val passengersTry = Try(sectionPassengersMap.get(section))
            var passengers = 0.0
            if (passengersTry.isFailure) {
              log.warn("exception occurred! {} Our approach is putting the timeKey as key and sectionPassengersMap as value to timeSectionTraffic", SectionNotExistException.MESSAGE)
              val newSectionPassengersMap = new util.HashMap[Section, lang.Double]()
              newSectionPassengersMap.put(section, passengers)
              timeSectionTraffic.put(timeKey, newSectionPassengersMap)
            } else {
              passengers = passengersTry.get
            }
            //            通过列车运行的到达时间和时段确定该时段通过的列车数
            val trainNumber = getTrainNum(timeKey, travelTimes)
            //            通过区间和列车运行所在区间Map集得到哪种类型的列车
            val train = sectionTrainMap.get(section)
            /*
                        以下就是正式进入费用计算的阶段
                         */
            var trainCost = 0.0
            if (trainNumber != 0) {
              trainCost = trainCostCompute(passengers / trainNumber, train)
            }
            pathCost += trainCost
            //            区间时间递增
            tempTime = DateExtendUtil.timeAdditionSecond(tempTime, seconds)
          }
          else {
            //         增加换乘时间，这里可能会有变动
            tempTime = DateExtendUtil.timeAdditionSecond(tempTime, TRANSFER_TIME.toInt)
          }
          //          每个区间都需要增加停车时间
          tempTime = DateExtendUtil.timeAdditionSecond(tempTime, STOP_STATION_TIME.toInt)
          pathWithTrainCrowdCost.put(pathWithId, pathCost)
          //          println(s"通过一次区间后的时间变为了$tempTime")
        })
      }
    )
    pathWithTrainCrowdCost
  }
}

object TrainCrowdCost {


  def main(args: Array[String]): Unit = {
    var temp = "2018-01-01 15:21:22"
    temp = DateExtendUtil.timeAdditionSecond(temp, 200)
    println(temp)
  }

}
