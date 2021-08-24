package control

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import calculate.{BaseCalculate, Logit}
import costcompute.{TimeIntervalStationFlow, TimeIntervalTraffic, TimeIntervalTransferFlow}
import dataload.{BaseDataLoad, HDFSODLoad}
import distribution.ODWithSectionResult
import domain.LinePassengers
import flowdistribute.OdWithTime
import kspcalculation.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import save._
import scala.collection.JavaConverters._
import scala.util.Try

class StaticController(sparkSession: SparkSession) extends Control {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  val checkPointPath = s"hdfs://hacluster/checkPoint/${new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())}"
  private var openOdShare = false

  /**
    * 静态分配参数类型，启动静态分配过程
    *
    * @param controlInfo 静态分配参数类型
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
    val sc = sparkSession.sparkContext
    //    k短路搜索服务对象应该用广播变量进行封装才好
    val kspNumber = controlInfo.getKspNumber
    val sectionInfoMap = sc.broadcast(baseDataLoad.getSectionInfoMap)
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    val baseCalculateBroad = sc.broadcast(new BaseCalculate(baseDataLoad, sectionTravelGraph))
    val odGetCost: RDD[(OdWithTime, Try[(util.List[Path], util.Map[Path, Double], (Path, Double))])] = odWithTimeRdd.map(odWithTime => {
      val baseCalculate = baseCalculateBroad.value
      val cost = Try(Control.tryCost(baseCalculate, kspNumber, odWithTime))
      if (cost.isFailure) {
        log.error(s"There is A problem in calculating the cost！ because {} and the od is $odWithTime", cost.failed.get.getMessage)
      }
      (odWithTime, cost)
    })
    odGetCost.persist()
    sparkSession.sparkContext.setCheckpointDir(checkPointPath)
    odGetCost.checkpoint()
    val errorOdFrame = odGetCost.filter(x => x._2.isFailure).map(x => x._1)
    //    得到无法分配的OD
    val errorOdSave = new ErrorOdSave(s"errorOd_${ControlInfo.startupTime()}")
    errorOdSave.saveByRdd(errorOdFrame, sparkSession)
    val odWithLegalPathAndResultRdd = odGetCost.filter(x => x._2.isSuccess).map(x => (x._1, x._2.get))
      .map(odAndCost => {
        val odWithTime = odAndCost._1
        val allCost = odAndCost._2
        val staticCost = allCost._2
        val minCost = allCost._3
        //        这里又要实例化BaseCalculate，消耗非常的大
        val logitResult = Logit.logit(staticCost, minCost, odWithTime.getPassengers)
        val lineFlowList = LineSave.lineFlow(logitResult, sectionInfoMap.value)
        val startTime = odWithTime.getInTime
        val timeInterval = controlInfo.getTimeInterval
        val result = Control.createDistributionResult()
        val tempResult = Control.createDistributionResult()
        val baseCalculate = baseCalculateBroad.value
        baseCalculate.distribute(logitResult, startTime, timeInterval, result, tempResult)
        //        log.error(s"1 at this section_result ${tempResult.getTimeIntervalTraffic}")
        (odWithTime, lineFlowList, result)
      })

    val linePassengersRdd: RDD[LinePassengers] = odWithLegalPathAndResultRdd.flatMap(x => x._2.asScala)
    odWithLegalPathAndResultRdd.map(x => println(x))
    val timeIntervalTrafficRdd: RDD[TimeIntervalTraffic] = odWithLegalPathAndResultRdd.map(x => {
      x._3.getTimeIntervalTraffic
    })
    val timeIntervalStationFlowRdd: RDD[TimeIntervalStationFlow] = odWithLegalPathAndResultRdd.map(x => {
      x._3.getTimeIntervalStationFlow
    })
    val timeIntervalTransferFlowRdd: RDD[TimeIntervalTransferFlow] = odWithLegalPathAndResultRdd.map(x => {
      x._3.getTimeIntervalTransferFlow
    })
    val lineSave = new LineSave(controlInfo.getLineTable)
    lineSave.saveByRdd(linePassengersRdd, sparkSession)
    val sectionSave = new SectionSave(controlInfo.getSectionTable, sectionInfoMap.value)
    sectionSave.saveByRdd(timeIntervalTrafficRdd, sparkSession)
    val stationSave = new StationSave(controlInfo.getStationTable)
    stationSave.saveByRdd(timeIntervalStationFlowRdd, sparkSession)
    val transferLineMap = baseDataLoad.getTransferLineMap
    val transferSave = new TransferSave(controlInfo.getTransferTable, transferLineMap)
    transferSave.saveByRdd(timeIntervalTransferFlowRdd, sparkSession)
    if (openOdShare) {
      val odWithSectionResultRdd: RDD[ODWithSectionResult] = odWithLegalPathAndResultRdd.map(x => {
        val odWithTime = x._1
        val timeIntervalTraffic = x._3.getTimeIntervalTraffic
        new ODWithSectionResult(odWithTime, timeIntervalTraffic)
      })
      val oDWithSectionSave = new ODWithSectionSave(controlInfo.getOdWithSectionSavePath, sectionInfoMap.value)
      oDWithSectionSave.saveByRdd(odWithSectionResultRdd, sparkSession)
    }
  }
}

object StaticController {
  def main(args: Array[String]): Unit = {
    testOnSpark(args)
  }

  def testOnSpark(args: Array[String]): Unit = {
    val odPath = args(0)
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val staticController = new StaticController(sparkSession)
    val controlInfo = new ControlInfo(odPath)
    staticController.startup(controlInfo)
  }

}