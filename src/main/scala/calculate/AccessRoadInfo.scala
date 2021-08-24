package calculate

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import dataload.Load
import dataload.base.{OracleChongQingLoad, OracleSectionLoad, OracleTransferLoad, SectionTravelTimeLoad}
import domain.Section
import entity.GraphWithSectionAssociation
import kspcalculation._
import org.apache.spark.sql.SparkSession

class AccessRoadInfo extends Serializable {
  private var oracleOrigin: String = _
  private var odPath: String = _
  private var distributionType: String = _
  private var lineResultPath: String = _
  private var sectionResultPath: String = _
  private var transferFee: Double = 5.0

  def this(odPath: String, distributionType: String, lineResultPath: String, sectionResultPath: String) {
    this()
    this.oracleOrigin = oracleOrigin
    this.odPath = odPath
    this.distributionType = distributionType
    this.lineResultPath = lineResultPath
    this.sectionResultPath = sectionResultPath
  }

  def this(odPath: String, distributionType: String, lineResultPath: String, sectionResultPath: String, transferFee: Double = 5.0) {
    this()
    this.oracleOrigin = oracleOrigin
    this.odPath = odPath
    this.distributionType = distributionType
    this.lineResultPath = lineResultPath
    this.sectionResultPath = sectionResultPath
    this.transferFee = transferFee
  }


  def sectionLoad(sectionLoad: Load, transferLoad: Load, sectionTravelTimeLoad: Load): GraphWithSectionAssociation = {
    val sectionFrame = sectionLoad.load()
    val transferFrame = transferLoad.load()
    val list = sectionFrame.collect
    val sectionOnlyMap = new java.util.HashMap[String, SectionStrData](700)
    val sectionInfoMap = new util.HashMap[String, SectionAllInfo](700)
    val sectionHashList = new java.util.ArrayList[SectionStrData](700)
    val sectionTravelTimeFrame = sectionTravelTimeLoad.load()
    val sectionTravelTimeWithRateMap = SectionTravelTimeLoad.getSectionTravelMap(sectionTravelTimeFrame)
    list.map(x => {
      val sectionId = x.getDecimal(0).intValue()
      val fromId = x.getDecimal(1).intValue().toString
      val toId = x.getDecimal(2).intValue().toString
      val travelTimeAndRate = sectionTravelTimeWithRateMap.get(new Section(fromId, toId))
      var weight = 1.0
      if (travelTimeAndRate != null) {
        weight = travelTimeAndRate.getSeconds.doubleValue() / 60
      } else {
        println(new Section(fromId, toId))
      }
      val direction = x.getDecimal(3).intValue().toString
      val lineName = x.getString(5)
      val sectionHash = new SectionStrData(sectionId, weight, fromId, toId, direction)
      sectionOnlyMap.put(s"$fromId $toId", sectionHash)
      val info = new SectionAllInfo(sectionId, weight, lineName, direction)
      sectionInfoMap.put(s"$fromId $toId", info)
      sectionHashList.add(sectionHash)
    })
    transferFrame.collect
      .map(x => {
        val sectionId = sectionHashList.size() + 1
        val weight = transferFee
        val fromId = x.getDecimal(0).intValue().toString
        val toId = x.getDecimal(1).intValue().toString
        val direction = "0"
        val sectionHash = new SectionStrData(sectionId, weight, fromId, toId, direction)
        val info = new SectionAllInfo(sectionId, weight, direction)
        sectionInfoMap.put(s"$fromId $toId", info)
        sectionHashList.add(sectionHash)
        sectionOnlyMap.put(s"$fromId $toId", sectionHash)
      })
    val edges = SectionStrData.getEdgesOfId(sectionHashList)
    val graph = new Graph(edges)
    val associationMap = new SectionAssociationMap(sectionInfoMap)
    val graphWithSectionAssociation = new GraphWithSectionAssociation(graph, associationMap, sectionTravelTimeWithRateMap)
    graphWithSectionAssociation
  }

  def odLoadToCompute(sparkSession: SparkSession, dataLoad: GraphWithSectionAssociation, odCutTime: OdCutTime): Unit = {
    val resultFrame = odCutTime.OdCutTimeActive(sparkSession)
    val sectionResult = resultFrame._2
    val date = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())
    val sectionPath = s"$sectionResultPath/$date"
    sectionResult.write.mode("append").csv(sectionPath + "/data")
    val lineResult = resultFrame._1
    val linePath = s"$lineResultPath/$date"
    val errorMessage = resultFrame._4
    lineResult.write.mode("append").csv(linePath + "/data")
    val odWithSectionFrame = resultFrame._3
    odWithSectionFrame.write.mode("append").csv(sectionPath + "/withOd")
    errorMessage.write.mode("append").csv(linePath + "/errorMessage")
  }
}

object AccessRoadInfo {
  def main(args: Array[String]): Unit = {
    runDistribution(args)
  }

  def runDistribution(args: Array[String]): Unit = {
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
    val lineResultPath = args(2)
    /*
        区间分配结果路径
        格式大致为 "hdfs://hacluster/od/data/sectionResult"
         */
    val sectionResultPath = args(3)
    /*
        换乘费用
        格式大致为 "TRANSFER_TIME:5"
         */
    val transferTime = args(4).split(":")(1).toDouble
    val kspNum = args(5).split(":")(1).toInt
    val accessRoadInfo = new AccessRoadInfo(odPath = odPath, distributionType = distributionType,
      lineResultPath = lineResultPath, sectionResultPath = sectionResultPath, transferFee = transferTime)
    val sectionLoad = new OracleSectionLoad()
    val sparkSession = sectionLoad.load().sparkSession
    val transferLoad = new OracleTransferLoad()
    val oracleCHONGQINGLoad = new OracleChongQingLoad()
    val sectionTravelTimeLoad = new SectionTravelTimeLoad()
    val association = accessRoadInfo.sectionLoad(sectionLoad, transferLoad, sectionTravelTimeLoad)
    val odCutTime = new OdCutTime(odOriginPath = odPath, distributionType = distributionType
      , graphWithSectionAssociation = association, kspNum = kspNum)
    accessRoadInfo.odLoadToCompute(sparkSession, association, odCutTime)
  }


}