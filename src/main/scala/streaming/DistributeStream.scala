package streaming

import conf.DynamicConf
import dataload.ODLoadByOracle
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class DistributeStream(sparkSession: SparkSession) extends StreamCompute {

  val dynamicParamConf = DynamicConf
  val distributionRequest: StructType = new StructType()
    .add("startTime", DataTypes.TimestampType)
    .add("endTime", DataTypes.TimestampType)
  val distributionTopic = "od-distribution"

  private def readDistributionRequest(): DataFrame = {
    import sparkSession.implicits._
    val kafkaStreaming = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
      .option("subscribe", distributionTopic)
      .option("startingOffsets", "latest")
      .option("enable.auto.commit", "false")
      .option("auto.commit.interval.ms", "1000")
      .load()
    val kafkaData = kafkaStreaming.selectExpr("CAST(key AS STRING)", "CAST (value AS STRING) as json")
      .as[(String, String)]
      //      过滤json
      .filter(x => JSON.parseFull(x._2).isDefined)
      //      对应schema信息，列别名取为od
      .select(from_json($"json", schema = distributionRequest).alias("request"))
      .select("request.startTime", "request.endTime")
    kafkaData
  }

  override def compute(): Unit = {
    import sparkSession.implicits._
    val requestFrame = readDistributionRequest()
    requestFrame.printSchema()
    val oDLoadByOracle = new ODLoadByOracle(sparkSession)
    val result = requestFrame.map(request => {
      val startTime = request.getTimestamp(0)
      val endTime = request.getTimestamp(1)
      val odRdd = oDLoadByOracle.getOdRdd(startTime)
      odRdd.foreach(println(_))
      1
    })
    requestFrame.writeStream.format("console").outputMode(OutputMode.Append()).start().awaitTermination()
  }
}

object DistributeStream {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DistributeStream").master("local[*]").getOrCreate()
    val distributeStream = new DistributeStream(sparkSession)
    distributeStream.compute()
  }
}
