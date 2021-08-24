package streaming

import java.sql.Timestamp
import java.{lang, util}

import calculate.{BaseCalculate, Logit}
import conf.DynamicConf
import control.Control
import costcompute._
import dataload.BaseDataLoad
import distribution.{DistributionResult, StationWithType}
import domain.Section
import flowdistribute.OdWithTime
import model._
import model.dto.{SectionLoadDTO, StationLoadDTO}
import model.vo.{NetLoadInfo, SectionLoad, StationLoad}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{explode, from_json, sum, window}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import utils.{DateExtendUtil, TimeKey, TravelTimeHandle}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.parsing.json.JSON

class KafkaStreamingCompute(sparkSession: SparkSession) extends StreamCompute {

  val odListSchema: StructType = new StructType()
    .add("odList", DataTypes.createArrayType(DataTypes.StringType))
  val oWithOutDSchema: StructType = new StructType()
    .add("inId", DataTypes.StringType)
    .add("passengers", DataTypes.DoubleType)
    .add("inTime", DataTypes.StringType)
  val odSchema: StructType = new StructType()
    .add("outId", DataTypes.StringType)
    .add("oWithOutD", oWithOutDSchema)
  val url = DynamicConf.localhostUrl
  val prop = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
  //    导入隐式转换
  import sparkSession.implicits._

  /**
    * 1.流式数据的读取可能存在多个
    * 2.流式数据的写出可能会有多个
    */
  override def compute(): Unit = {
    val dynamicParamConf = DynamicConf
    val kafkaStreaming = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
      .option("subscribe", dynamicParamConf.topics(0))
      .option("startingOffsets", "latest")
      .option("enable.auto.commit", "false")
      .option("auto.commit.interval.ms", "1000")
      .load()
    val kafkaData = kafkaStreaming.selectExpr("CAST(key AS STRING)", "CAST (value AS STRING) as json")
      .as[(String, String)]
      //      过滤json
      .filter(x => JSON.parseFull(x._2).isDefined)
      //      对应schema信息，列别名取为od
      .select(from_json($"json", schema = odListSchema).alias("odList"))
      .select(explode($"odList.odList"))
      .select(from_json($"col", schema = odSchema).alias("od"))
      .select("od.oWithOutD.inId", "od.outId", "od.oWithOutD.inTime", "od.oWithOutD.passengers")
    //      //      得到json数据的子对象值
    //      .select("od.inId", "od.outId", "od.inTime", "od.outTime", "od.passengers")
    kafkaData.printSchema()
    val baseDataLoad = new BaseDataLoad
    //    创建列车运行时刻表图
    val sectionTravelGraph = baseDataLoad.getSectionTravelGraph
    val trainOperationSectionBroad = sparkSession.sparkContext.broadcast(baseDataLoad.getTrainOperationSection.getSectionTrainMap)
    //    获取所有区间在某个时间通过列车的数量
    var trainPassSectionMap: Map[Section, Map[TimeKey, mutable.Buffer[TravelTime]]] = Map()
    sectionTravelGraph.getSectionListMap.asScala.foreach(x => {
      val section = x._1
      val timeKeyContainTravelTimeMap: Map[TimeKey, mutable.Buffer[TravelTime]] = x._2.asScala.groupBy(x => {
        TravelTimeHandle.getTimeKey(x, dynamicParamConf.timeInterval)
      })
      trainPassSectionMap += section -> timeKeyContainTravelTimeMap
    })
    val trainPassSectionMapBroad = sparkSession.sparkContext.broadcast(trainPassSectionMap)
    val afcTransferIdMapBroad = sparkSession.sparkContext.broadcast(baseDataLoad.getAfcTransferIdMap)
    val baseCalculateBroad = sparkSession.sparkContext.broadcast(new BaseCalculate(baseDataLoad, sectionTravelGraph))
    sparkSession.sparkContext.broadcast()
    var distributionResult = sparkSession.sparkContext.broadcast(Control.createDistributionResult())
    val timeLoadFrame: Dataset[(List[TimeWithStationLoad], List[TimeWithSectionLoad])] = kafkaData.map(
      od => {
        val afcInId = od.getString(0)
        val afcOutId = od.getString(1)
        val inTime = od.getString(2)
        val passengers = od.getDouble(3)
        //        val oDWithTimeModel = ODWithTimeModel(inId, outId, inTime, outTime)
        val afcTransferIdMap = afcTransferIdMapBroad.value
        val inId = afcTransferIdMap.get(afcInId)
        val outId = afcTransferIdMap.get(afcOutId)
        val odWithTime = new OdWithTime(inId, outId, inTime, passengers)
        //        得到K短路
        val pathList = baseCalculateBroad.value.getLegalPathList(dynamicParamConf.pathNum, odWithTime)
        //        得到静态费用
        val staticCost = baseCalculateBroad.value.getStaticCost(pathList)
        //        得到最小费用
        val minCost = new MinGeneralizedCost().compose(staticCost)
        //        创建临时集合
        val tempResult = Control.createDistributionResult()
        Range(0, 2).foreach(
          _ => {
            //            动态费用计算
            val dynamicCost = baseCalculateBroad.value.dynamicCalculate(odWithTime, staticCost, minCost, pathList, distributionResult.value.getTimeIntervalTraffic, 15)
            //            logit模型分配
            val logitResult = Logit.logit(dynamicCost, minCost, passengers)
            //            动态分配
            baseCalculateBroad.value.distribute(logitResult, inTime, dynamicParamConf.timeInterval, distributionResult.value, tempResult)
          }
        )
        val timeWithStationLoads: List[TimeWithStationLoad] = getStationLoad(distributionResult, tempResult).toList
        val timeWithSectionLoads: List[TimeWithSectionLoad] = getSectionLoad(distributionResult, tempResult,
          trainPassSectionMapBroad.value, trainOperationSectionBroad.value).toList
        //    2.Driver端定义实体，实体用HashMap来表示
        (timeWithStationLoads, timeWithSectionLoads)
        //        假如有两个OD，那么这两个OD可能产生的netLoadInfo可能会有重合的部分，这个时候又需要聚合
      }
    )
    timeLoadFrame.createOrReplaceTempView("timeLoad")
    val stationDelay = DynamicConf.stationDelay
    val timeWithStationFrame = timeLoadFrame.flatMap(x => x._1)
      .select($"timeKeyModel.startTime".cast(DataTypes.TimestampType).as("startTime"), $"stationLoad")
      .withWatermark("startTime", stationDelay)
    val sectionDelay = DynamicConf.sectionDelay
    val timeWithSectionFrame = timeLoadFrame.flatMap(x => x._2)
      .select($"timeKeyModel.startTime".cast(DataTypes.TimestampType).as("startTime"), $"sectionLoad")
      .withWatermark("startTime", sectionDelay)
    //  如何将stationFrame聚合为一个List集合呢？
    var netLoadInfoMap: Map[String, NetLoadInfo] = Map()
    val windowDuration = DynamicConf.windowDuration
    val slideDuration = DynamicConf.slideDuration
    //    定义一个清空广播变量的时间，然后重新广播
    var minAndMaxDateUpdate = ("2018-06-13 00:00:00", "2018-06-13 00:00:00")

    val querySection = timeWithSectionFrame.writeStream.foreachBatch((batch: DataFrame, batchId: Long) => {
      val queryStation = timeWithStationFrame.writeStream.foreachBatch((batch: DataFrame, batchId: Long) => {
        //      这里开始聚合
        var stationLoadMap: Map[String, List[StationLoad]] = Map()
        /*
           1.将两个流式数据分别聚合
           2.在driver端定义对象实体
           3.遍历两个流式数据集，对数据集进行遍历后得到该对象的值，这里牵涉到网络传输通信
           4.将该对象交给spark,通过spark操作kafkaAPI发送
        */
        val stationWindow = batch.groupBy(window($"startTime", windowDuration, slideDuration),
          $"stationLoad.stationId")
          .agg(sum("stationLoad.avgVolume") as "avgVolume", sum("stationLoad.passengers") as "passengers", sum("stationLoad.inVolume") as "inVolume", sum("stationLoad.outVolume") as "outVolume", sum("stationLoad.crowdingRate") as "crowdingRate")
          .select("window.start", "stationId", "passengers", "avgVolume", "inVolume", "outVolume", "crowdingRate")
        stationWindow.write.mode(SaveMode.Append).jdbc(url, "STATION_WINDOW", prop)
        stationWindow.collect.foreach(x => {
          val startTime = NetWorkLoadStream.timeStampToString(x.getTimestamp(0))
          val stationId = x.getString(1).toInt
          val avgVolume = x.getDouble(2).toInt
          val inVolume = x.getDouble(3).toInt
          val outVolume = x.getDouble(4).toInt
          val passengers = x.getDouble(5).toInt
          val crowdingRate = x.getDouble(6)
          val stationLoad = StationLoad(stationId, avgVolume, inVolume, outVolume, passengers, crowdingRate)
          if (stationLoadMap.contains(startTime)) {
            var stationLoads = stationLoadMap(startTime)
            stationLoads = stationLoads :+ stationLoad
            stationLoadMap += (startTime -> stationLoads)
          } else {
            val stationLoads = List(stationLoad)
            stationLoadMap += (startTime -> stationLoads)
          }
        })
        stationLoadMap.foreach(x => {
          val startTime = x._1
          val stationLoads = x._2
          val netLoadInfo = NetLoadInfo(startTime, stationLoads, List())
          netLoadInfoMap += (startTime -> netLoadInfo)
        })
      }).start()
      batch.printSchema()
      //      这里开始聚合
      var sectionLoadMap: Map[String, List[SectionLoad]] = Map()
      val sectionWindow = batch.groupBy(window($"startTime", windowDuration, slideDuration),
        $"sectionLoad.startId", $"sectionLoad.endId", $"sectionLoad.trainNum", $"sectionLoad.passengers")
        .agg(sum("sectionLoad.volume") as "volume", sum("sectionLoad.utilizationRate") as "utilizationRate")
        .select("window.start", "startId", "endId", "volume", "trainNum", "passengers", "utilizationRate")
      sectionWindow.collect.foreach(x => {
        val startTime = NetWorkLoadStream.timeStampToString(x.getTimestamp(0))
        if ("".equals(minAndMaxDateUpdate._1) || "".equals(minAndMaxDateUpdate._2)) {
          minAndMaxDateUpdate = (startTime, startTime)
        } else {
          val i = minAndMaxDateUpdate._2.compareTo(startTime)
          if (i > 0) {
            minAndMaxDateUpdate = (minAndMaxDateUpdate._1, startTime)
          }
        }
        val startId = x.getString(1).toInt
        val endId = x.getString(2).toInt
        val volume = x.getDouble(3).toInt
        val trainNum = x.getDouble(4).toInt
        val passengers = x.getDouble(5).toInt
        val utilizationRate = x.getDouble(6)
        val sectionLoad = SectionLoad(startId, endId, volume, trainNum, passengers, utilizationRate)
        if (sectionLoadMap.contains(startTime)) {
          var sectionLoads = sectionLoadMap(startTime)
          sectionLoads = sectionLoads :+ sectionLoad
          sectionLoadMap += (startTime -> sectionLoads)
        } else {
          val sectionLoads = List(sectionLoad)
          sectionLoadMap += (startTime -> sectionLoads)
        }
      })
      sectionLoadMap.foreach(x => {
        val startTime = x._1
        val sectionLoads = x._2
        if (netLoadInfoMap.contains(startTime)) {
          val netLoadInfo = netLoadInfoMap(startTime)
          var sectionLoads = netLoadInfo.SectionLoads
          sectionLoads = sectionLoads ::: sectionLoads
          netLoadInfoMap += startTime -> netLoadInfo
        } else {
          val netLoadInfo = NetLoadInfo(startTime, List(), sectionLoads)
          netLoadInfoMap += startTime -> netLoadInfo
        }
      })

      val netLoadInfoFrame = netLoadInfoMap.values.toList.toDF("RecordTime", "StationLoads", "SectionLoads")
      netLoadInfoFrame.show()
      netLoadInfoFrame
        .toJSON
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
        .option("topic", dynamicParamConf.topics(2))
        .save()
      //      这里更新广播变量，如果变化的时间超过一天
      if (DateExtendUtil.timeDifference(minAndMaxDateUpdate._1, minAndMaxDateUpdate._2, DateExtendUtil.HOUR) >= 24) {
        distributionResult.unpersist()
        distributionResult = sparkSession.sparkContext.broadcast(Control.createDistributionResult())
        minAndMaxDateUpdate = ("", "")
      }
      netLoadInfoMap = Map()
    }).start()
    querySection.awaitTermination()
  }

  /**
    * 如何计算车的数量？通过区间->得到时间,在计算多少时间在timeKey内，在特定时间段内，通过的车数量
    *
    * @param distributionResult 分配的所有结果
    * @param tempResult         分配的暂时结果
    * @return
    */
  private def getSectionLoad(distributionResult: Broadcast[DistributionResult], tempResult: DistributionResult, trainPassSectionMap: Map[Section, Map[TimeKey, mutable.Buffer[TravelTime]]],
                             trainOperationSection: java.util.Map[Section, Train]) = {
    val sectionTraffic = distributionResult.value.getTimeIntervalTraffic.getTimeSectionTraffic
    val timeWithSectionLoads = tempResult.getTimeIntervalTraffic.getTimeSectionTraffic.asScala.flatMap(timeSection => {
      val timeKey = timeSection._1
      val timeWithOutDate = TimeKey.getOnlyTime(timeKey)
      val timeSectionMap = timeSection._2.asScala.map(sectionFlow => {
        val section = sectionFlow._1
        val flow = sectionFlow._2
        val passengers = sectionTraffic.get(timeKey).get(section)
        val travelTimes = trainPassSectionMap(section)
        val arrivalNum = travelTimes(timeWithOutDate)
        val trainNum = arrivalNum.size
        val train = trainOperationSection.get(section)
        val utilizationRate = NetWorkLoadStream.computeUtilizationRate(flow, train, trainNum)
        val timeWithSectionLoad = TimeWithSectionLoad(TimeKeyModel(timeKey.getStartTime, timeKey.getEndTime),
          SectionLoadDTO(section.getInId, section.getOutId, flow, trainNum, passengers, utilizationRate))
        timeWithSectionLoad
      })
      timeSectionMap
    })
    timeWithSectionLoads
  }

  private def getStationLoad(distributionResult: Broadcast[DistributionResult], tempResult: DistributionResult) = {
    val dynamicAllTimeStationTraffic: util.Map[TimeKey, util.Map[StationWithType, lang.Double]] = distributionResult.value.getTimeIntervalStationFlow.getTimeStationTraffic
    val timeWithStationLoads = tempResult.getTimeIntervalStationFlow.getTimeStationTraffic.asScala.flatMap(timeStation => {
      val timeKey = timeStation._1
      val stationFlowMap = timeStation._2
      val stationLoads = stationFlowMap.asScala.map(stationFlow => {
        val station = stationFlow._1
        val flow = stationFlow._2
        if ("in".equals(station.getType)) {
          val stationLoad = StationLoadDTO(station.getStationId, flow, flow, 0, flow, flow / 3000)
          TimeWithStationLoad(TimeKeyModel(timeKey.getStartTime, timeKey.getEndTime), stationLoad)
        } else {
          val stationLoad = StationLoadDTO(station.getStationId, flow, 0, flow, flow, flow / 3000)
          TimeWithStationLoad(TimeKeyModel(timeKey.getStartTime, timeKey.getEndTime), stationLoad)
        }
      })
      stationLoads
    })
    timeWithStationLoads
  }


}

object KafkaStreamingCompute {
  def main(args: Array[String]): Unit = {
    SparkSession.builder().master("local[*]").getOrCreate()
    val sparkSession = SparkSession.builder().appName("KafkaStreamingCompute").getOrCreate()
    val kafkaStreamingCompute = new NetWorkLoadStream(sparkSession)
    kafkaStreamingCompute.compute()
  }

  def timeStampToString(timestamp: Timestamp): String = {
    import java.text.SimpleDateFormat
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val standardDate = sdf.format(timestamp)
    standardDate
  }

  def computeUtilizationRate(passengers: Double, train: Train, trainNum: Int): Double = {
    passengers / train.getMaxCapacity * trainNum
  }
}
