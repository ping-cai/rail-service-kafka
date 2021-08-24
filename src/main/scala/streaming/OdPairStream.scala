package streaming

import conf.DynamicConf
import org.apache.spark.sql.functions.{explode, from_json, min, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.parsing.json.JSON

class OdPairStream(sparkSession: SparkSession) extends StreamCompute {


  val AFCSchema: StructType = new StructType()
    //    票卡号
    .add("ticketId", DataTypes.StringType)
    //    交易时间
    .add("transTime", DataTypes.TimestampType)
    //    交易事件
    .add("transEvent", DataTypes.StringType)
    //    交易车站ID
    .add("transCode", DataTypes.StringType)
  val AFCListSchema: StructType = new StructType().add("afcList", DataTypes.createArrayType(DataTypes.StringType))
  val url = DynamicConf.localhostUrl
  val prop = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
  //    导入隐式转换
  import sparkSession.implicits._

  //  od配对系统topic
  private val odPairTopic = DynamicConf.topics(1)
  //od配对系统水印时间
  private val odPairDelay: String = DynamicConf.odPairDelay

  /**
    * OD配对系统,topic为od-Pair
    * 1.流式数据的读取可能存在多个
    * 2.流式数据的写出可能会有多个
    */
  override def compute(): Unit = {
    sparkSession.sparkContext.setLogLevel("WARN")
    val dynamicParamConf = DynamicConf
    val kafkaStreaming = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
      .option("subscribe", odPairTopic)
      .option("startingOffsets", "latest")
      .option("enable.auto.commit", "false")
      .option("auto.commit.interval.ms", "1000")
      .load()
    val kafkaData = kafkaStreaming.selectExpr("CAST(key AS STRING)", "CAST (value AS STRING) as json")
      .as[(String, String)]
      //      过滤json
      .filter(x => JSON.parseFull(x._2).isDefined)
      //      对应schema信息，列别名取为od
      .select(from_json($"json", AFCListSchema).alias("afcList"))
      .select(explode($"afcList.afcList"))
      .select(from_json($"col", schema = AFCSchema).alias("afc"))
      //      //      得到json数据的子对象值
      .select("AFC.ticketId", "AFC.transTime", "AFC.transEvent", "AFC.transCode")
    val kafkaQuery = kafkaData.writeStream.format("console").outputMode(OutputMode.Append()).start()
    kafkaData.createOrReplaceTempView("afc")
    //    34进站,35出站
    val selectInStation = "select ticketId id,transTime date,transCode in_station from afc where transEvent='21'"
    val selectOutStation = "select ticketId id,transTime date,transCode out_station from afc where transEvent='22'"
    val inStationFrame = sparkSession.sql(selectInStation).withWatermark("date", odPairDelay)
    val outStationFrame = sparkSession.sql(selectOutStation).withWatermark("date", odPairDelay)

    inStationFrame.createOrReplaceTempView("in_station")
    outStationFrame.createOrReplaceTempView("out_station")
    val odPairSql = "select origin.id,origin.date inTime,origin.in_station,target.date outTime,target.out_station from in_station origin join out_station target " +
      "where origin.id=target.id"
    val odFrame = sparkSession.sql(odPairSql)
    //    val username = dynamicParamConf.localhostUser
    //    val password = dynamicParamConf.localhostPassword
    //    val pairWriter = new OdPairWriter("oracle.jdbc.driver.OracleDriver", url, username, password)
    //    val odPairSink = odFrame.withWatermark("inTime", odPairDelay)
    //      .groupBy(window($"inTime", "15 minutes"), $"in_station", $"out_station")
    //      .count()
    //      .select($"window.start" as "START_TIME", $"in_station" as "IN_ID", $"out_station" as "OUT_ID", $"count" as "PASSENGERS")
    //      .writeStream.foreach(pairWriter).outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime("2 seconds")).start()
    //
    //    val odPairQuery = odFrame.writeStream.format("console")
    //      .option("truncate", "false")
    //      .outputMode(OutputMode.Append())
    //      .trigger(Trigger.ProcessingTime("2 seconds"))
    //      .start()
    //    odPairSink.awaitTermination()
    //    odPairQuery.awaitTermination()
    val odFrameQuery = odFrame.withWatermark("inTime", odPairDelay).writeStream.foreachBatch((batch: DataFrame, batchId: Long) => {
      //      获取配对数据
      val odPairFrame = batch.select("in_station", "inTime", "out_station", "outTime")
      val odCountFrame = odPairFrame.groupBy(window($"inTime", "15 minutes"), $"in_station", $"out_station")
        .count().select($"window.start" as "START_TIME", $"in_station" as "IN_ID", $"out_station" as "OUT_ID", $"count" as "PASSENGERS")
      if (odPairFrame.count() > 0) {
        val tableNameDateSet = odPairFrame.select("inTime")
          .agg(min($"inTime" cast DataTypes.LongType) cast DataTypes.TimestampType as "inTime")
          .select("inTime").map(x => {
          val date = x.getTimestamp(0).toLocalDateTime
          val localDate = date.toLocalDate
          val year = localDate.getYear - 2000
          val month = localDate.getMonthValue
          val week = localDate.getDayOfMonth / 7
          (year, month, week)
        })
        val tableNameTuple = tableNameDateSet.collect()(0)
        val year = tableNameTuple._1
        val month = tableNameTuple._2
        val week = tableNameTuple._3
        odCountFrame.write.mode(SaveMode.Append).jdbc(url, s"KALMAN_OD_${year}_${month}_$week", prop)
      }
      odPairFrame.show()
    }).outputMode(OutputMode.Append()).start()
    kafkaQuery.awaitTermination()
    odFrameQuery.awaitTermination()
  }
}

object OdPairStream {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("StaticStreamingCompute").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val staticStreamingCompute = new OdPairStream(sparkSession)
    staticStreamingCompute.compute()
  }
}
