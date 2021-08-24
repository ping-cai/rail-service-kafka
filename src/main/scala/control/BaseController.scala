package control

import java.util

import calculate.BaseCalculate
import costcompute.MinGeneralizedCost
import dataload.{BaseDataLoad, HDFSODLoad}
import distribution.ODWithSectionResult
import domain.LinePassengers
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import save._

import scala.util.Try

class BaseController(sparkSession: SparkSession) extends Control {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var openOdShare = false

  /**
    * 控制动态分配的参数类型
    *
    * @param controlInfo 动态分配控制信息
    */
  override def startup(controlInfo: ControlInfo): Unit = {
    openOdShare = controlInfo.isOpenShareFunction
    val odSourcePath = controlInfo.getOdSourcePath
    val hDFSODLoad = new HDFSODLoad(odSourcePath)
    val odWithTimeRdd = hDFSODLoad.getOdRdd(sparkSession)
    /*
        od是分布式的，如果进行动态分配，一定会有线程安全问题
        并且shuffle过程会严重影响性能，故分布式应该先计算出所有的路径，对路径进行reduce,再进行单线程分配
         */
    /*
    这里的路网数据应该用广播变量，否则每一个task都会拉取一个路网数据，网络开销非常大
     */
    val baseDataLoad = new BaseDataLoad
    //    k短路搜索服务对象应该用广播变量进行封装才好
    val kspNumber = controlInfo.getKspNumber
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    val sc = sparkSession.sparkContext
    val baseCalculateBroad = sc.broadcast(new BaseCalculate(baseDataLoad, sectionTravelGraph))
    val tupleList = odWithTimeRdd.map(odWithTime => {
      val baseCalculate = baseCalculateBroad.value

      def tryCost = {
        val legalPath = baseCalculate.getLegalPathList(kspNumber, odWithTime)
        val staticCost = baseCalculate.getStaticCost(legalPath)
        val minGeneralizedCost = new MinGeneralizedCost().compose(staticCost)
        (legalPath, staticCost, minGeneralizedCost)
      }

      val cost = Try(tryCost)
      if (cost.isFailure) {
        log.error(s"There is A problem in calculating the cost！ because {} and the od is $odWithTime", cost.failed.get.getMessage)
      }
      (odWithTime, cost)
    }).filter(x => x._2.isSuccess).map(x => (x._1, x._2.get)).collect()
    val distributionResult = Control.createDistributionResult()
    val timeIntervalTraffic = distributionResult.getTimeIntervalTraffic
    val timeIntervalStationFlow = distributionResult.getTimeIntervalStationFlow
    val timeIntervalTransferFlow = distributionResult.getTimeIntervalTransferFlow
    val sectionInfoMap = baseDataLoad.getSectionInfoMap
    val timeInterval = controlInfo.getTimeInterval
    val lineFlowResult = new util.ArrayList[LinePassengers]()
    var odWithResult = new util.ArrayList[ODWithSectionResult]()
    if (openOdShare) {
      odWithResult = new util.ArrayList[ODWithSectionResult](100000000)
    }
    tupleList.foreach(x => {
      val baseCalculate = baseCalculateBroad.value
      val odWithTime = x._1
      val cost = x._2
      val legalPath = cost._1
      val staticCost = cost._2
      val minCost = cost._3
      val tempResult = Control.createDistributionResult()
      Range(0, 2).foreach(_ => {
        val logitResult = baseCalculate.dynamicCalculate(odWithTime, staticCost, minCost, legalPath, timeIntervalTraffic, timeInterval)
        //    线路流量的获取：线路名，区间获得线路名，需要logit模型分配后的区间流量，不包含有时段信息，区间->线路Map集合
        baseCalculate.distribute(logitResult, odWithTime.getInTime, timeInterval, distributionResult, tempResult)
        val lineFlowList = LineSave.lineFlow(logitResult, sectionInfoMap)
        lineFlowResult.addAll(lineFlowList)
      })
      if (openOdShare) {
        odWithResult.add(new ODWithSectionResult(odWithTime, tempResult.getTimeIntervalTraffic))
      }
    })

    val lineSave = new LineSave(controlInfo.getLineTable)
    lineSave.save(lineFlowResult, sparkSession)
    val sectionSave = new SectionSave(controlInfo.getSectionTable, sectionInfoMap)
    sectionSave.save(timeIntervalTraffic, sparkSession)
    val stationSave = new StationSave(controlInfo.getStationTable)
    stationSave.save(timeIntervalStationFlow, sparkSession)
    val transferLineMap = baseDataLoad.getTransferLineMap
    val transferSave = new TransferSave(controlInfo.getTransferTable, transferLineMap)
    transferSave.save(timeIntervalTransferFlow, sparkSession)
    if (openOdShare) {
      val oDWithSectionSave = new ODWithSectionSave(controlInfo.getOdWithSectionSavePath, sectionInfoMap)
      oDWithSectionSave.save(odWithResult, sparkSession)
    }
  }

}

object BaseController {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DynamicDistribution").getOrCreate()
    //    val odSourcePath = "G:/Destop/lowOd.csv"
    //    val controlInfo = new ControlInfo(odSourcePath, 3)
    val controlInfo = setControlInfo(args)
    val baseController = new BaseController(sparkSession)
    baseController.startup(controlInfo)
  }

  def setControlInfo(args: Array[String]): ControlInfo = {
    /*
      od数据地址
      格式大致为 "hdfs://hacluster/od/data/diff.csv"
       */
    val odPath = args(0)
    //    分配类型
    /*
        线路分配结果路径
        格式大致为 "distributionType:static"
         */
    val distributionType = args(1).split(":")(1)
    /*
        线路分配结果路径
        格式大致为 "hdfs://hacluster/od/data/lineResult"
         */
    val kspNum = args(2).split(":")(1).toInt

    val controlInfo = new ControlInfo(odPath, distributionType, kspNum)
    controlInfo
  }

}


