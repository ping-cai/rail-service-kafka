package batch.back

import java.util
import java.util.Properties

import conf.DynamicConf
import dataload.BaseDataLoad
import domain.Section
import kspcalculation.{Path, PathService}
import model.back._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

class PathSaveService(baseDataLoad: BaseDataLoad, pathNum: Int, sparkSession: SparkSession) extends KPathTrait {
  private val sectionWithDirectionMap: util.Map[Section, String] = baseDataLoad.getSectionWithDirectionMap
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val pathService = new PathService(pathNum, baseDataLoad.getGraph, sectionWithDirectionMap)

  import sparkSession.implicits._

  //    换乘次数偏离约束
  val transferTimesDeviation: Int = DynamicConf.transferTimesDeviation
  //    出行费用放大系数约束
  val travelExpenseAmplification: Double = DynamicConf.travelExpenseAmplification
  //    出行费用最大值约束
  val maximumTravelCost: Double = DynamicConf.maximumTravelCost

  override def compute(odModel: ODModel): List[PathModel] = {
    val legalKPath = pathService.getLegalKPath(odModel.inId, odModel.outId)
    legalKPath.asScala.flatMap(path => {
      val edges = path.getEdges
      var totalCost = 0.0
      val edgeModelList = edges.asScala.flatMap(edge => {
        totalCost += edge.getWeight
        List(EdgeModel(edge.getFromNode, edge.getToNode, edge.getWeight))
      }).toList
      List(PathModel(edgeModelList, totalCost))
    }).toList
  }

  def getPathWithIdList(odModel: ODModel): ListBuffer[PathWithId] = {
    val legalKPathTry = Try(pathService.getLegalKPath(odModel.inId, odModel.outId))
    if (legalKPathTry.isSuccess) {
      val legalKPath = legalKPathTry.get
      log.warn("path is live!showing the message", legalKPath)
      val pathWithTransferBuffer = legalKPath.asScala.map(path => {
        var tempTransferTime = 0
        val edgeModels = ListBuffer[EdgeModel]()
        path.getEdges.forEach(edge => {
          val section = Section.createSectionByEdge(edge)
          if (null == sectionWithDirectionMap.get(section)) {
            tempTransferTime += 1
          }
          edgeModels append EdgeModel(edge.getFromNode, edge.getToNode, edge.getWeight)
        })
        PathWithTransfer(path, tempTransferTime)
      })
      val minCost = pathWithTransferBuffer.minBy(x => x.path.getTotalCost).path.getTotalCost
      val minTransferTime = pathWithTransferBuffer.minBy(x => x.transferTime).transferTime
      val tempResult = ListBuffer[Path]()
      pathWithTransferBuffer foreach (pathWithTransfer => {
        val path = pathWithTransfer.path
        val cost = path.getTotalCost
        val transferCount = pathWithTransfer.transferTime
        if (cost <= ((1 + travelExpenseAmplification) * minCost) &&
          cost <= (minCost + maximumTravelCost) &&
          transferCount <= (minTransferTime + transferTimesDeviation)) {
          tempResult append path
        }
      })
      val pathInfoSorted = tempResult.sortBy(x => x.getTotalCost)
      val pathWithIdBuffer = ListBuffer[PathWithId]()
      pathInfoSorted.indices.foreach(x => {
        val inId = (odModel.inId.toInt + 1000).toString
        val outId = (odModel.outId.toInt + 1000).toString
        val pathId = s"$inId$outId$x"
        val pathWithId = PathWithId(pathId, pathInfoSorted(x))
        pathWithIdBuffer.append(pathWithId)
      })
      pathWithIdBuffer
    } else {
      log.error("An exception occurred!And the od is {} ", odModel)
      ListBuffer[PathWithId]()
    }
  }

  def compute2PathList(odModel: ODModel): List[PathDetails] = {
    val legalKPathTry = Try(pathService.getLegalKPath(odModel.inId, odModel.outId))
    if (legalKPathTry.isSuccess) {
      val legalKPath = legalKPathTry.get
      val sectionInfoMap = baseDataLoad.getSectionInfoMap
      val pathInfoBuffer = legalKPath.asScala.map(path => {
        val lineSet = mutable.HashSet[String]()
        var tempTransferTime = 0
        val edgeModels = ListBuffer[EdgeModel]()
        path.getEdges.forEach(edge => {
          val section = Section.createSectionByEdge(edge)
          if (null == sectionWithDirectionMap.get(section)) {
            tempTransferTime += 1
          } else {
            val sectionInfo = sectionInfoMap.get(section)
            val line = sectionInfo.getLine
            lineSet.add(line)
          }
          edgeModels append EdgeModel(edge.getFromNode, edge.getToNode, edge.getWeight)
        })
        val totalCost = path.getTotalCost
        val lineSetString = lineSet.mkString("->")
        val pathModel = PathModel(edgeModels.toList, totalCost)
        PathInfo(pathModel, totalCost, tempTransferTime, lineSetString)
      })
      val minCost = pathInfoBuffer.minBy(x => x.cost).cost
      val minTransferTime = pathInfoBuffer.minBy(x => x.transferCount).transferCount
      val tempResult = ListBuffer[PathInfo]()
      pathInfoBuffer foreach (x => {
        if (x.cost <= ((1 + travelExpenseAmplification) * minCost) &&
          x.cost <= (minCost + maximumTravelCost) &&
          x.transferCount <= (minTransferTime + transferTimesDeviation)) {
          tempResult append x
        }
      })
      val pathInfoSorted = tempResult.sortBy(x => x.cost)
      val pathDetailsBuffer = pathInfoSorted.indices.map(x => {
        val inId = (odModel.inId.toInt + 1000).toString
        val outId = (odModel.outId.toInt + 1000).toString
        val pathInfo = pathInfoSorted(x)
        val pathId = s"$inId$outId$x"
        PathDetails(pathId, odModel.inId, odModel.outId, pathInfo)
      })
      pathDetailsBuffer.toList
    } else {
      List[PathDetails]()
    }
  }

  def path2Edges(pathId: String, pathInfo: PathInfo): List[EdgeDetails] = {
    val edgeModelList = pathInfo.path.edgeModel
    val result = ListBuffer[EdgeDetails]()
    var tempWeight = 0.0
    val firstEdgeModel = edgeModelList.head
    var firstNode = firstEdgeModel.fromNode
    val lastEdgeModel = edgeModelList.last
    val lastNode = lastEdgeModel.toNode
    edgeModelList.indices.foreach(index => {
      val edgeModel = edgeModelList(index)
      val section = new Section(edgeModel.fromNode, edgeModel.toNode)
      if (null == sectionWithDirectionMap.get(section)) {
        result append EdgeDetails(pathId, result.size, EdgeModel(firstNode, edgeModel.fromNode, tempWeight))
        firstNode = edgeModel.toNode
        tempWeight = 0.0
      } else {
        tempWeight += edgeModel.weight
      }
    })
    result append EdgeDetails(pathId, result.size, EdgeModel(firstNode, lastNode, tempWeight))
    result.toList
  }

  def savePathResult(odModel: ODModel): Unit = {
    val pathList = compute(odModel)
    val pathResult = pathList.indices.map(pathId => {
      ODWithPath(odModel.inId, odModel.outId, pathId, pathList(pathId).toString)
    })
    val url = DynamicConf.localhostUrl
    val prop = new Properties()
    prop.put("user", DynamicConf.localhostUser)
    prop.put("password", DynamicConf.localhostPassword)
    val dataFrame = pathResult.toDF()
    dataFrame.printSchema()
    dataFrame.write.mode(SaveMode.Append).jdbc(url, "OD_PATH", prop)
  }

  def saveFrame(resultFrame: DataFrame, saveTable: String): Unit = {
    val url = DynamicConf.localhostUrl
    val prop = new Properties()
    prop.put("user", DynamicConf.localhostUser)
    prop.put("password", DynamicConf.localhostPassword)
    resultFrame.write.mode(SaveMode.Append).jdbc(url, saveTable, prop)
  }


}
