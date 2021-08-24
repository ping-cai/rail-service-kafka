package calculate

import java.{lang, util}

import costcompute._
import dataload.{BaseDataLoad, HDFSODLoad, Load}
import distribution.{DistributionResult, StaticLogitDistribute, StationWithType, TransferWithDirection}
import domain.Section
import domain.dto.SectionResult
import flowdistribute.OdWithTime
import kspcalculation.{Graph, Path, PathService}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.TimeKey

import scala.collection.JavaConverters._
class StaticCalculate extends Serializable {
  def calculate(baseDataLoad: BaseDataLoad, timeIntervalTraffic: TimeIntervalTraffic, odWithTime: OdWithTime, interval: Int): DistributionResult = {
    val graph: Graph = baseDataLoad.getGraph
    //    这里的东西可以写入配置文件
    val pathComputeService = new PathService(5, graph, baseDataLoad.getSectionWithDirectionMap)
    //    一定要去除首尾有换乘的站点
    val pathRemoved = pathComputeService.getLegalKPath(odWithTime.getInId, odWithTime.getOutId)
    //    区间运行图的创建
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    //    区间运行费用
    val sectionMoveCost = new SectionMoveCost(sectionTravelGraph)
    val sectionMovePathCost = sectionMoveCost.compose(pathRemoved)
    //    列车停站费用
    val stopTimeCost = new StopTimeCost()
    val stopTimePathCost = stopTimeCost.compose(pathRemoved)

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
    //    sectionMovePathCost.forEach((path, cost) =>
    //      println(s"路径是$path 最初的费用是$cost"))
    //最小广义费用
    val minGeneralizedCost = new MinGeneralizedCost().compose(sectionMovePathCost)
    //    allPathCost.forEach((path, cost) =>
    //      println(s"路径是$path 总费用是$cost")
    //    )
    val timeIntervalStationMap = new util.HashMap[TimeKey, util.Map[StationWithType, java.lang.Double]]()
    val timeIntervalStationFlow = new TimeIntervalStationFlow(timeIntervalStationMap)
    val timeIntervalTransferMap = new util.HashMap[TimeKey, util.Map[TransferWithDirection, lang.Double]]()
    val timeIntervalTransferFlow = new TimeIntervalTransferFlow(timeIntervalTransferMap)
    val result = new DistributionResult(timeIntervalTraffic, timeIntervalStationFlow, timeIntervalTransferFlow)
    val staticDistribute = new StaticLogitDistribute(allPathCost, minGeneralizedCost, result, baseDataLoad)
    val distributionResult: DistributionResult = staticDistribute.logit(odWithTime.getPassengers, odWithTime.getInTime)
    distributionResult
  }
}

object StaticCalculate {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val baseDataLoad = new BaseDataLoad
    val timeSectionTraffic = new util.HashMap[TimeKey, util.Map[Section, java.lang.Double]]()
    val intervalTraffic = new TimeIntervalTraffic(timeSectionTraffic)
    val interval = 15
    val HDFSPath = "G:/Destop/OD.csv"
    val hDFSODLoad = new HDFSODLoad(HDFSPath)
    val odWithTimeRdd = hDFSODLoad.getOdRdd(sparkSession)
    val staticCalculate = new StaticCalculate
    val resultRdd = odWithTimeRdd.map(odWithTime => {
      staticCalculate.calculate(baseDataLoad, intervalTraffic, odWithTime, interval)
    })
    val sectionResultRdd: RDD[SectionResult] = resultRdd.flatMap(result => {
      val sectionTraffic = result.getTimeIntervalTraffic.getTimeSectionTraffic
      val sectionResultIterable = sectionTraffic.asScala.flatMap(timeKeyWithMap => {
        val timeKey = timeKeyWithMap._1
        val sectionMap = timeKeyWithMap._2.asScala
        val resultIterable = sectionMap.map(sectionFlow => {
          val section = sectionFlow._1
          val flow = sectionFlow._2
          SectionResult(timeKey.getStartTime, timeKey.getEndTime,
            section.getInId, section.getOutId, flow)
        })
        resultIterable
      })
      sectionResultIterable
    })
    import sparkSession.implicits._
    val sectionFrame = sectionResultRdd.toDF()
    sectionFrame.createOrReplaceTempView("sectionResult")
    val sumFlowSql = "select startTime START_TIME,endTime END_TIME,startStationId START_STATION_ID,endStationId END_STATION_ID,cast(sum(passengers) as int) PASSENGERS from sectionResult group by startTime,endTime,startStationId,endStationId"
    val resultFrame = sparkSession.sql(sumFlowSql)
    val url = Load.url
    val prop = Load.prop
    resultFrame.write.mode("append").jdbc(url, "SECTION_GRAPH_TEST", prop)
  }
}
