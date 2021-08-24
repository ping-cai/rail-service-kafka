package streaming

import java.util.Properties

import calculate.{BaseCalculate, Logit}
import conf.DynamicConf
import control.Control
import costcompute.MinGeneralizedCost
import dataload.{BaseDataLoad, Load}
import distribution.{DistributionResult, StationWithType, TransferWithDirection}
import domain.dto._
import flowdistribute.OdWithTime
import org.apache.spark.sql.functions.{min, sum}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
  * Kafka接受OD数据消息
  * 3条K短路径搜索
  * 静态分配
  *
  */
class SecurityIndicatorsStream(sparkSession: SparkSession) extends StreamCompute {
  //导入隐式转换
  import sparkSession.implicits._

  private val url: String = DynamicConf.localhostUrl
  private val prop: Properties = Load.prop
  private val pathNum: Int = DynamicConf.pathNum
  private val dynamicParamConf = DynamicConf
  //  安全指标系统的topic为security-indicators
  private val securityIndicatorsTopic = dynamicParamConf.topics(3)

  override def compute(): Unit = {
    val readOdStream = new ReadOdStream(sparkSession)
    val odStream = readOdStream.readStream()
    //    打印odStream的列名信息
    odStream.printSchema()
    /*
    inId,outId,inTime,passengers
     */
    //    加载全部基础数据
    val baseDataLoad = new BaseDataLoad
    //    静态分配需要数据
    //    区间运行图
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    //    可计算K短路
    val baseCalculate = new BaseCalculate(baseDataLoad, sectionTravelGraph)
    val sc = sparkSession.sparkContext
    val baseCalculateBroad = sc.broadcast(baseCalculate)
    val afcTransferIdMap = baseDataLoad.getAfcTransferIdMap
    val afcTransferIdMapBroad = sc.broadcast(afcTransferIdMap)
    val resultEncoder = Encoders.kryo[DistributionResult]
    val distributionResult = odStream.filter(od => {
      //      过滤出一定在区间运行图上的OD
      afcTransferIdMapBroad.value.containsKey(od.getString(0)) && afcTransferIdMapBroad.value.containsKey(od.getString(1))
    })
      .map(od => {
        //       AFC进站ID
        val inId = od.getString(0)
        //        AFC出站ID
        val outId = od.getString(1)
        //        进站时间
        val inTime = od.getString(2)
        //        进站人数
        val passengers = od.getDouble(3)
        //        转换成系统进站id
        val inStation = afcTransferIdMap.get(inId)
        //        转换成系统出站id
        val outStation = afcTransferIdMap.get(outId)
        //        创建odWithTime对象
        val odWithTime = new OdWithTime(inStation, outStation, inTime, passengers)
        //        得到最短路径
        val pathList = baseCalculateBroad.value.getLegalPathList(pathNum, odWithTime)
        //        得到静态费用
        val staticCost = baseCalculateBroad.value.getStaticCost(pathList)
        //        得到最小费用
        val minCost = new MinGeneralizedCost().compose(staticCost)
        //        Logit分配结果
        val logitResult = Logit.logit(staticCost, minCost, passengers)
        //        最终结果
        val result = Control.createDistributionResult()
        //        创建临时集合
        val tempResult = Control.createDistributionResult()
        val distributionWithTemp = baseCalculateBroad.value.distribute(logitResult, inTime, DynamicConf.timeInterval, result, tempResult)
        distributionWithTemp.getTempResult
      })(resultEncoder)
    distributionResult.writeStream.foreachBatch(
      (batchFrame: Dataset[DistributionResult], batchId: Long) => {
        batchFrame.persist()
        val sectionFrame = outputIntervalSet(batchFrame).toDF()
        sectionFrame.printSchema()
        //        SectionResult(START_TIME: String, END_TIME: String, START_STATION_ID: String, END_STATION_ID: String, PASSENGERS: Double)
        val sectionAggFrame = sectionFrame.groupBy("START_TIME", "END_TIME", "START_STATION_ID", "END_STATION_ID").agg(sum("PASSENGERS") as "PASSENGERS")
        //        StationDataFrame(STATION_ID: String, START_TIME: String, END_TIME: String, ENTRY_QUATITY: Double, EXIT_QUATITY: Double, ENTRY_EXIT_QUATITY: Double)
        val stationFrame = outputStationSet(batchFrame).toDF()
        val stationAggFrame = stationFrame.groupBy("STATION_ID", "START_TIME", "END_TIME").agg(sum("ENTRY_QUATITY") as "ENTRY_QUATITY", sum("EXIT_QUATITY") as "EXIT_QUATITY", sum("ENTRY_EXIT_QUATITY") as "ENTRY_EXIT_QUATITY")
        val transferFrame = outputTransferSet(batchFrame).toDF()
        //        TransferResult(START_TIME: String, END_TIME: String, XFER_STATION_ID: String, TRANS_QUATITY: Double)
        val transferAggFrame = transferFrame.groupBy("START_TIME", "END_TIME", "XFER_STATION_ID").agg(sum("TRANS_QUATITY") as "TRANS_QUATITY")
        sectionAggFrame.write.mode(SaveMode.Append).jdbc(url, "STREAM_SECTION", prop)
        stationAggFrame.write.mode(SaveMode.Append).jdbc(url, "STREAM_STATION", prop)
        val minTimeFrame = sectionFrame.agg(min($"START_TIME" cast DataTypes.TimestampType cast DataTypes.LongType) as "startTime")
        minTimeFrame.select($"startTime" cast DataTypes.TimestampType).toJSON
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
          .option("topic", securityIndicatorsTopic)
          .save()
        batchFrame.unpersist()
        transferAggFrame.write.mode(SaveMode.Append).jdbc(url, "STREAM_TRANSFER", prop)
      }).outputMode(OutputMode.Append()).start().awaitTermination()

  }

  private def outputIntervalSet(batchFrame: Dataset[DistributionResult]): Dataset[SectionResult] = {
    val result = batchFrame.flatMap(x => {
      x.getTimeIntervalTraffic.getTimeSectionTraffic.asScala.flatMap(sectionTraffic => {
        val timeKey = sectionTraffic._1
        val startTime = timeKey.getStartTime
        val endTime = timeKey.getEndTime
        val sectionMap = sectionTraffic._2
        sectionMap.asScala.map(sectionFlow => {
          val section = sectionFlow._1
          val flow = sectionFlow._2
          SectionResult(startTime, endTime, section.getInId, section.getOutId, flow)
        })
      })
    })
    result
  }

  private def outputTransferSet(batchFrame: Dataset[DistributionResult]): Dataset[TransferResult] = {
    val result = batchFrame.flatMap(x => {
      x.getTimeIntervalTransferFlow.getTimeIntervalTransferFlow.asScala.flatMap(transferTraffic => {
        val timeKey = transferTraffic._1
        val startTime = timeKey.getStartTime
        val endTime = timeKey.getEndTime
        transferTraffic._2.asScala.map(transferMap => {
          val transferWithDirection: TransferWithDirection = transferMap._1
          val flow = transferMap._2
          val section = transferWithDirection.getSection
          val id = section.getInId
          TransferResult(startTime, endTime, id, flow)
        })
      })
    })
    result
  }

  private def outputStationSet(batchFrame: Dataset[DistributionResult]): Dataset[StationDataFrame] = {
    val result = batchFrame.flatMap(x => {
      x.getTimeIntervalStationFlow.getTimeStationTraffic.asScala.flatMap(stationTraffic => {
        val timeKey = stationTraffic._1
        val startTime = timeKey.getStartTime
        val endTime = timeKey.getEndTime
        val stationMap = stationTraffic._2
        stationMap.asScala.map(stationFlow => {
          val stationWithType: StationWithType = stationFlow._1
          val flow = stationFlow._2
          if ("in".equals(stationWithType.getType)) {
            StationDataFrame(stationWithType.getStationId, startTime, endTime, flow, 0, flow)
          } else {
            StationDataFrame(stationWithType.getStationId, startTime, endTime, 0, flow, flow)
          }
        })
      })
    })
    result
  }
}

object SecurityIndicatorsStream {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("SecurityIndicatorsStream").getOrCreate()
    val staticSecurityMetrics = new SecurityIndicatorsStream(sparkSession)
    staticSecurityMetrics.compute()
  }
}