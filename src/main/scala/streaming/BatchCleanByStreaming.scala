package streaming

import batch.BatchClean
import conf.DynamicConf
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class BatchCleanByStreaming(sparkSession: SparkSession) extends StreamCompute {
  val cleanCommandSchema: StructType = StructType(
    Array(
      StructField("originFilePath", StringType, nullable = true),
      StructField("startDate", StringType, nullable = true),
      StructField("endDate", StringType, nullable = true),
      StructField("targetFilePath", StringType, nullable = true)
    )
  )


  val dynamicParamConf: DynamicConf.type = DynamicConf
  //    导入隐式转换
  import sparkSession.implicits._


  override def compute(): Unit = {
    val afcCleanCommand = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
      .option("subscribe", dynamicParamConf.afcCleanTopic(0))
      .option("startingOffsets", "latest")
      .option("enable.auto.commit", "false")
      .option("auto.commit.interval.ms", "1000")
      .load()
    val command = afcCleanCommand.selectExpr("CAST(key AS STRING)", "CAST (value AS STRING) as json")
      .as[(String, String)]
      //      过滤json
      .filter(x => JSON.parseFull(x._2).isDefined)
      //      对应schema信息，列别名取为od
      .select(from_json($"json", schema = cleanCommandSchema).alias("cleanCommand"))
      .select("cleanCommand.originFilePath", "cleanCommand.startDate", "cleanCommand.endDate", "cleanCommand.targetFilePath")
    command.writeStream.foreachBatch((batch: DataFrame, batchId: Long) => {
      batch.show()
      batch.collect().foreach(x => {
        val originFilePath = x.getString(0)
        val startDate = x.getString(1)
        val endDate = x.getString(2)
        val targetFilePath = x.getString(3)
        new BatchClean(sparkSession).batchClean(originFilePath, startDate, endDate, targetFilePath)
      })
    }).outputMode(OutputMode.Append()).start().awaitTermination()
  }


}

object BatchCleanByStreaming {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("BatchCleanByStreaming").master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val batchCleanByStreaming = new BatchCleanByStreaming(sparkSession)
    batchCleanByStreaming.compute()
  }

  def toDateTime(date: String, time: String): String = {
    if (date.length < 8 || time.length < 4) {
      val dateTime = String.format("%s %s", date, time)
      dateTime
    } else {
      val year = date.substring(0, 4)
      val month = date.substring(4, 6)
      val day = date.substring(6, 8)
      val hour = time.substring(0, 2)
      val minute = time.substring(2, 4)
      val second = date.substring(4, 6)
      val dateTime = String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, minute, second)
      dateTime
    }
  }
}
