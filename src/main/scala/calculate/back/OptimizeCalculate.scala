package calculate.back

import java.sql.Timestamp

import batch.back.PathSaveService
import calculate.Logit
import conf.DynamicConf
import costcompute.{MinGeneralizedCost, SectionTravelGraph, StaticCost}
import dataload.BaseDataLoad
import dataload.back.BackSectionLoad
import domain.Section
import flowdistribute.OdWithTime
import model.back._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class OptimizeCalculate(sparkSession: SparkSession, baseDataLoad: Broadcast[BaseDataLoad], backSectionLoad: Broadcast[BackSectionLoad]) extends Serializable {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val stationStopTime: Double = DynamicConf.stationStopTime
  private val transferTime: Double = DynamicConf.transferTime

  //  计算路径出来
  def computePathList(odWithTimeRdd: RDD[OdWithTime], pathSaveService: Broadcast[PathSaveService]): RDD[(OdWithTime, ListBuffer[PathWithId])] = {
    val result = odWithTimeRdd.map(odWithTime => {
      val inId = odWithTime.getInId
      val outId = odWithTime.getOutId
      val oDWithTimeModel = ODWithTimeModel(inId, outId, odWithTime.getInTime, "")
      //      计算得到路径结果
      val pathWithIdBuffer: ListBuffer[PathWithId] = pathSaveService.value.getPathWithIdList(oDWithTimeModel)
      (odWithTime, pathWithIdBuffer)
    }).filter(x => x._2.nonEmpty)
    result
  }

  def getResult(odWithTime: OdWithTime, pathWithIdBuffer: ListBuffer[PathWithId]): (ListBuffer[BackPushFirstResult], ListBuffer[BackPushSecondResult]) = {
    //      计算费用
    val sectionWithDirectionMap = baseDataLoad.value.getSectionWithDirectionMap
    val staticCost = new StaticCost(sectionWithDirectionMap)
    val sectionTravelGraph = baseDataLoad.value.getSectionTravelGraph
    val sectionExtraMap = baseDataLoad.value.getSectionExtraMap
    val pathWithCost = staticCost.costCompute(pathWithIdBuffer, sectionTravelGraph, sectionExtraMap)
    val minGeneralizedCost = new MinGeneralizedCost
    val minCost = minGeneralizedCost.compose(pathWithCost)
    val passengers = odWithTime.getPassengers
    val pathWithIdAndPassengers = Logit.logit(pathWithCost, minCost, passengers)
    val inTime = odWithTime.getInTime
    val inTimestamp = Timestamp.valueOf(inTime)
    val sectionSeqMap = backSectionLoad.value.sectionSeqMap
    val result = calculate(pathWithIdAndPassengers, inTimestamp, sectionTravelGraph, sectionSeqMap)
    val firstResult = result._1
    val backPushFirstBuffer = firstResult.map(x => {
      val inId = odWithTime.getInId
      val outId = odWithTime.getOutId
      val seqSize = x.sectionSeqSize
      BackPushFirstResult(inId, outId, x.time, x.sectionSeq, x.seqFlow / seqSize, passengers / seqSize)
    })
    val secondResult = result._2
    val backPushSecondBuffer = secondResult.map(x => {
      val inId = odWithTime.getInId
      val outId = odWithTime.getOutId
      val time = x.inTime
      val sectionSeq = x.sectionSeq
      val notPassedSectionSeqPathId = x.notPassedSectionSeqPathId
      val pathFlow = x.pathFlow
      BackPushSecondResult(inId, outId, time, sectionSeq, notPassedSectionSeqPathId, pathFlow)
    })
    (backPushFirstBuffer, backPushSecondBuffer)
  }


  private def calculate(logitResult: mutable.HashMap[PathWithId, Double], inTime: Timestamp,
                        sectionTravelGraph: SectionTravelGraph,
                        sectionSeqMap: mutable.HashMap[Int, mutable.HashSet[Section]]): (ListBuffer[SectionSeqInfo], ListBuffer[UnPassedSeqPathInfo]) = {
    //    分配logit模型

    //    1.对每个区间进行分配
    //    2.寻找出现区段的个数
    //    3.寻找可能连续的区段
    //    4.判断是否有一条路径经过了连续区段
    //    5.判断是否有其他路径不经过所有的连续区段
    //    总结果
    val sectionSeqBuffer: ListBuffer[SectionSeqInfo] = ListBuffer[SectionSeqInfo]()
    //    存储经过的路径ID
    val passedPath = mutable.HashSet[String]()
    //  存储经过的区段ID
    val saveSeq = mutable.HashSet[Int]()
    //    第一次循环得到第一次的结果
    logitResult.foreach(x => {
      val pathWithId = x._1
      val path = pathWithId.path
      var currentTime = inTime
      val flow = x._2
      val passedSeq = mutable.HashSet[Int]()
      val sectionBuffer = path.getEdges.asScala.map(edge => {
        val section = Section.createSectionByEdge(edge)
        sectionSeqMap.foreach(seqAndSet => {
          val seq = seqAndSet._1
          val sectionSet = seqAndSet._2
          if (sectionSet.contains(section)) {
            passedSeq.add(seq)
            saveSeq.add(seq)
          }
        })
        section
      })
      //      为做交集
      val allSectionSet = sectionBuffer.toSet
      //      判断区间是否经过区段
      if (passedSeq.nonEmpty) {
        log.info("The passedSeqSize is {}", passedSeq.size)
        passedSeq.foreach(seq => {
          val sectionSet = sectionSeqMap(seq)
          val interSet = allSectionSet.intersect(sectionSet)
          val interSize = interSet.size
          //          判断是否经过完整的连续区段
          if (interSize == sectionSet.size) {
            log.info("The interSize is {}", interSize)
            //           输出第一个结果
            allSectionSet.foreach(section => {
              val seconds = sectionTravelGraph.getTravelTime(section)
              if (sectionSet.contains(section)) {
                log.info("save the sectionSet!and the section is {}", section)
                sectionSeqBuffer.append(SectionSeqInfo(currentTime, seq, interSize, flow))
              }
              //              输出第二个结果
              passedPath.add(pathWithId.pathId)
              passedSeq.add(seq)
              //              如果是换乘
              if (null == seconds) {
                val addTime = currentTime.getTime + (transferTime * 1000).toInt + (stationStopTime * 1000).toInt
                currentTime = new Timestamp(addTime)
              } else {
                // 若不是换乘
                val addTime = currentTime.getTime + seconds * 1000 + (stationStopTime * 1000).toInt
                currentTime = new Timestamp(addTime)
              }
            })
          }
        })
      }
    })
    //    第二次循环得到第二次的结果，依赖于第一次
    log.error("the allPathSize is {}", logitResult.size)
    log.error("the passedPathSize is {}", passedPath.size)
    val unPassedSeqBuffer: ListBuffer[UnPassedSeqPathInfo] = ListBuffer[UnPassedSeqPathInfo]()
    logitResult.foreach(x => {
      val pathWithId = x._1
      val flow = x._2
      if (!passedPath.contains(pathWithId.pathId)) {
        log.error("there is a path not pass seq!message is {}", pathWithId)
        log.error("the saveSeqSize is {}", saveSeq.size)
        saveSeq.foreach(seq => {
          val unPassedSeqPathInfo = UnPassedSeqPathInfo(inTime, seq, pathWithId.pathId, flow)
          unPassedSeqBuffer.append(unPassedSeqPathInfo)
        })
      }
    })
    (sectionSeqBuffer, unPassedSeqBuffer)
  }

}
