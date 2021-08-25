package batch

import java.sql.Timestamp
import java.text.SimpleDateFormat

import model.ODWithFlow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.window
import utils.TimestampUtil

class BatchClean2HDFS(sparkSession: SparkSession) extends Serializable {
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

  /**
    * TICKET_ID,TXN_DATE,TXN_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID
    *
    * @param dataFrame afc数据
    */
  def clean(dataFrame: DataFrame, startInt: Int, endInt: Int, targetPath: String): Unit = {
    sparkSession.sparkContext.getConf.set("spark.default.parallelism", "20")
    dataFrame.persist()
    dataFrame.createOrReplaceTempView("afc_origin")
    sparkSession.udf.register("to_date_time", (date: String, time: String) => toDateTime(date, time))
    val cycleTime = endInt.toInt - startInt
    for (currentDate <- 0 to cycleTime) {
      val endInt = startInt + currentDate
      val date = endInt.toString
      val preNeedSql = s"select trim(TICKET_ID) TICKET_ID,TXN_DATE,TXN_TIME,TRANS_CODE,TXN_STATION_ID from afc_origin where TXN_DATE=$date"
      val matchBeforeFrame = sparkSession.sql(preNeedSql)
      matchBeforeFrame.createOrReplaceTempView("afc")

      val inStationFrame = sparkSession.sql(
        """
select TICKET_ID,row_number() over(partition by TICKET_ID order by TXN_DATE,TXN_TIME) SEQ_ID,TXN_DATE,TXN_TIME,TXN_STATION_ID
FROM afc
WHERE TRANS_CODE=21
        """.stripMargin)
      val outStationFrame = sparkSession.sql(
        """
          |select TICKET_ID,row_number() over(partition by TICKET_ID order by TXN_DATE,TXN_TIME) SEQ_ID,TXN_DATE,TXN_TIME,TXN_STATION_ID
          |FROM afc
          |WHERE TRANS_CODE=22
        """.stripMargin)
      inStationFrame.createOrReplaceTempView("in_afc")
      outStationFrame.createOrReplaceTempView("out_afc")
      val odMatchFrame = sparkSession.sql(
        """
select origin.TICKET_ID,origin.TXN_DATE origin_date,origin.TXN_TIME origin_time,origin.TXN_STATION_ID origin_station_id,
destination.TXN_DATE destination_date,destination.TXN_TIME destination_time,destination.TXN_STATION_ID destination_station_id
FROM in_afc origin join out_afc destination
on origin.TICKET_ID=destination.TICKET_ID and origin.SEQ_ID = destination.SEQ_ID
        """.stripMargin)
      odMatchFrame.createOrReplaceTempView("od")
      val resultFrame = sparkSession.sql(
        """
select TICKET_ID,to_date_time(origin_date,origin_time) in_time,origin_station_id in_id,
to_date_time(destination_date,destination_time) out_time,destination_station_id out_id
from od
        """.stripMargin)
      val odResultFilePath = s"/odPair/$date"
      resultFrame.coalesce(1).write.mode("append").csv(targetPath + odResultFilePath)
    }
  }

  def odAggregation(odFilePath: String, startInt: Int, endInt: Int, targetPath: String): Unit = {
    import sparkSession.implicits._
    val odSchema: StructType = StructType(Array(
      StructField("in_ticket", StringType, nullable = true),
      StructField("in_time", StringType, nullable = true),
      StructField("in_id", StringType, nullable = true),
      StructField("out_time", StringType, nullable = true),
      StructField("out_id", StringType, nullable = true)
    )
    )
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (currentDate <- startInt to endInt) {
      val odResultFilePath = s"/odPair/$currentDate"
      val odFrame = sparkSession.read.schema(odSchema).csv(odFilePath + odResultFilePath).select("in_time", "in_id", "out_id")
      val odAggregationFrame = odFrame.select("in_time", "in_id", "out_id")
        .map(x => {
          val inTime = x.getString(0)
          val timestamp = TimestampUtil.timeAgg(Timestamp.valueOf(inTime))
          val finalInTime = dateFormat.format(timestamp)
          val inId = x.getString(1)
          val outId = x.getString(2)
          ODWithFlow(inId, outId, finalInTime, 1)
        }).select("inId", "outId", "inTime", "passengers")
        .groupBy("inId", "outId", "inTime").count()
      val odAggregationPath = s"/odAggregation/$currentDate"
      odAggregationFrame.coalesce(1).write.mode("append").csv(targetPath + odAggregationPath)
    }
  }

  def batchClean(dataFrame: DataFrame, startDate: Int, endDate: Int, targetPath: String): Unit = {
    clean(dataFrame, startDate, endDate, targetPath)
    odAggregation(targetPath, startDate, endDate, targetPath)
  }
}

object BatchClean2HDFS {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("BatchClean2HDFS").master("local[*]").getOrCreate()
    val batchClean2HDFS = new BatchClean2HDFS(sparkSession)
    batchClean2HDFS.odAggregation("D:/Destop", 20210224, 20210224, "D:/Destop/")
  }
}