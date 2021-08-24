package dataload

import java.util

import conf.DynamicConf
import costcompute._
import dataload.base._
import domain.param.{AdjustParam, CostParam}
import domain.{Section, SectionInfo}
import kspcalculation.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * @author LiYongPing
  *         加载最基本的数据源，此处并未加载OD数据表，因为OD数据是系统外部数据，需要经过转换才可成为系统内部数据
  * 1.加载区间表
  * 2.加载换乘表
  * 3.加载车站表
  * 4.加载运行图时刻表
  */
class BaseDataLoad extends Serializable with AdjustParam[Double] {
  private var sectionList: java.util.ArrayList[SectionInfo] = _
  private var transferList: java.util.ArrayList[SectionInfo] = _
  private var afcTransferIdMap: util.Map[String, String] = _
  private var trainOperationSection: TrainOperationSection = _
  private var sectionWithDirectionMap: java.util.Map[Section, String] = _
  private var sectionInfoMap: java.util.Map[Section, SectionInfo] = _
  private var transferLineMap: java.util.Map[String, String] = _
  private var sectionTravelGraph: SectionTravelGraph = _
  private var TRANSFER_TIME: Double = CostParam.TRANSFER_TIME
  private val TransferPenalty: Double = CostParam.TRANSFER_PENALTY
  private var SECTION_WEIGHT_ADD_RATE = CostParam.SECTION_WEIGHT_ADD_RATE
  private var SectionExtraMap: mutable.HashMap[Section, Double] = _
  private var graph: Graph = _
  baseLoadInit()

  def getSectionList: java.util.ArrayList[SectionInfo] = {
    this.sectionList
  }

  def getTransferList: java.util.ArrayList[SectionInfo] = {
    this.transferList
  }

  def getAfcTransferIdMap: util.Map[String, String] = {
    this.afcTransferIdMap
  }

  def getTrainOperationSection: TrainOperationSection = {
    this.trainOperationSection
  }

  def getSectionInfoMap: util.Map[Section, SectionInfo] = {
    this.sectionInfoMap
  }

  def getTransferLineMap: java.util.Map[String, String] = {
    this.transferLineMap
  }

  def getSectionExtraMap: mutable.HashMap[Section,Double] = {
    this.SectionExtraMap
  }

  def getSectionTravelGraph: SectionTravelGraph = {
    this.sectionTravelGraph
  }


  def initSectionExtraWeight(sectionExtra: OracleExtraSectionWeightLoad): Unit = {
    val dataFrame = sectionExtra.load()
    //    初始化hash集合
    val sectionMap = mutable.HashMap[Section, Double]()
    //    从executor端拉去到driver端
    dataFrame.collect().map(x => {
      val inId = x.getString(0)
      val outId = x.getString(1)
      val extraWeight = x.getDecimal(2).doubleValue()
      sectionMap.put(new Section(inId, outId), extraWeight)
    })
    this.SectionExtraMap = sectionMap
  }

  private def baseLoadInit(): Unit = {
    val sectionLoad = new OracleSectionLoad()
    val transferLoad = new OracleTransferLoad()
    val chongQingLoad = new OracleChongQingLoad()
    val sectionFrame = sectionLoad.load()
    val sectionList = new util.ArrayList[SectionInfo](700)
    //    那就先得到区间运行时间，再存入K短路集合
    //    初始化运行图时刻表
    val sectionTravelTimeLoad = new OracleSectionTravelTimeLoad()
    val sectionTravelTimeLoadFrame = sectionTravelTimeLoad.load()
    val TravelTimeList = sectionTravelTimeLoad.getSectionTravelTimeList(sectionTravelTimeLoadFrame)
    val sectionTravelGraph = SectionTravelGraph.createTravelGraph(TravelTimeList)
    this.sectionTravelGraph = sectionTravelGraph
    //    不包含换乘虚拟区间的方向信息
    val sectionDirectionMap = new util.HashMap[Section, String]()
    //    添加区间信息进入sectionInfoList中
    //    添加真实区间及其所有信息Map
    val sectionInfoMap = new util.HashMap[Section, SectionInfo]
    sectionFrame.rdd.collect.map(x => {
      val section = new Section(x.getDecimal(1).intValue().toString,
        x.getDecimal(2).intValue().toString)
      val sectionInfo = new SectionInfo(
        x.getDecimal(0).intValue(),
        section,
        x.getDecimal(3).intValue().toString,
        //        这个即为区间运行时分，单位为分钟
        sectionTravelGraph.getTravelTime(section) / 60.0,
        x.getString(5))
      sectionList.add(sectionInfo)
      sectionInfoMap.put(sectionInfo.getSection, sectionInfo)
      sectionDirectionMap.put(sectionInfo.getSection, sectionInfo.getDirection)
    })
    this.sectionInfoMap = sectionInfoMap
    this.sectionWithDirectionMap = sectionDirectionMap
    val transferList = new util.ArrayList[SectionInfo](150)
    val transferFrame = transferLoad.load()
    val transferLineMap = new util.HashMap[String, String]()
    transferFrame.rdd.collect.map(x => {
      val section = new Section(x.getDecimal(0).intValue().toString,
        x.getDecimal(1).intValue().toString)
      val inLineName = x.getString(2)
      val outLineName = x.getString(3)
      val sectionInfo = new SectionInfo(
        0,
        section,
        "0",
        //        这里的换乘点设置惩罚
        TransferPenalty,
        s"$inLineName $outLineName")
      transferLineMap.put(section.getInId, inLineName)
      transferLineMap.put(section.getOutId, outLineName)
      transferList.add(sectionInfo)
    })
    this.transferLineMap = transferLineMap
    val afcTransferFrame = chongQingLoad.load()
    val afcTransferIdMap = new util.HashMap[String, String](300)
    afcTransferFrame.rdd.collect.map(
      x => {
        val afcId = x.getString(0)
        val stationId = x.getDecimal(1).intValue().toString
        afcTransferIdMap.put(afcId, stationId)
      }
    )
    this.sectionList = sectionList
    this.transferList = transferList
    this.afcTransferIdMap = afcTransferIdMap
    // 初始化路网图
    val edges = Edge.getEdgeBySections(sectionList)
    edges.addAll(Edge.getEdgeBySections(transferList))
    val graph = Graph.createGraph(edges)
    this.graph = graph
    //    初始化区间列车通行表
    val sectionTrain = new OracleSectionTrain()
    val sectionTrainFrame = sectionTrain.load()
    val sectionToTrain = new util.HashMap[Section, Train]
    sectionTrainFrame.rdd.collect.map(
      x => {
        val section = new Section(x.getDecimal(0).intValue().toString,
          x.getDecimal(1).intValue().toString)
        val train = new Train(x.getString(2),
          x.getDecimal(3).intValue(),
          x.getDecimal(4).intValue())
        sectionToTrain.put(section, train)
      }
    )
    val trainOperationSection = new TrainOperationSection(sectionToTrain)
    this.trainOperationSection = trainOperationSection
    //    增加这个区间权值修改功能
    if (DynamicConf.openSectionAdjust) {
      initSectionExtraWeight(new OracleExtraSectionWeightLoad)
    }
  }

  def getGraph: Graph = {
    this.graph
  }

  def getSectionWithDirectionMap: java.util.Map[Section, String] = {
    this.sectionWithDirectionMap
  }

  override def adjust(param: Double): Unit = {
    TRANSFER_TIME = param
  }
}

object BaseDataLoad {
  def main(args: Array[String]): Unit = {
    testInit()
  }

  def testInit(): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val baseDataLoad = new BaseDataLoad()
    baseDataLoad.getSectionList.forEach(x => {
      if (x.getWeight > 900) {
        println(x)
      }
    })
  }
}
