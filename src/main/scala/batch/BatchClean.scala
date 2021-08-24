package batch

import java.util.Properties

import conf.DynamicConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import streaming.BatchCleanByStreaming

class BatchClean(sparkSession: SparkSession) extends Serializable {
  val afcOriginSchema: StructType = StructType(
    Array(
      StructField("TICKET_ID", StringType, nullable = true),
      StructField("TXN_DATE", StringType, nullable = true),
      StructField("TXN_TIME", StringType, nullable = true),
      StructField("CARD_COUNTER", StringType, nullable = true),
      StructField("STL_DATE", StringType, nullable = true),
      StructField("FILE_ID", StringType, nullable = true),
      StructField("TICKET_MAIN_TYPE", StringType, nullable = true),
      StructField("TICKET_TYPE", StringType, nullable = true),
      StructField("MEDIA_TYPE", StringType, nullable = true),
      StructField("VER_NO", StringType, nullable = true),
      StructField("TICKET_CSN", StringType, nullable = true),
      StructField("TRANS_CODE", StringType, nullable = true),
      StructField("TXN_STATION_ID", StringType, nullable = true),
      StructField("LAST_STATION_ID", StringType, nullable = true),
      StructField("PAY_TYPE", StringType, nullable = true),
      StructField("BEFORE_AMT", StringType, nullable = true),
      StructField("TXN_AMT", StringType, nullable = true),
      StructField("TXN_NUM", StringType, nullable = true),
      StructField("REWARD_AMT", StringType, nullable = true),
      StructField("DEP_AMT", StringType, nullable = true),
      StructField("CARD_BAL", StringType, nullable = true),
      StructField("CARD_TXN_FLG", StringType, nullable = true),
      StructField("RSN_CODE", StringType, nullable = true),
      StructField("RSN_DATE", StringType, nullable = true),
      StructField("TRANS_STAT", StringType, nullable = true),
      StructField("ORIG_TICKET_ID", StringType, nullable = true),
      StructField("DEV_CODE", StringType, nullable = true),
      StructField("DEV_SEQ", StringType, nullable = true),
      StructField("SAM_ID", StringType, nullable = true),
      StructField("OPERATOR_ID", StringType, nullable = true),
      StructField("SALE_DEV_CODE", StringType, nullable = true),
      StructField("CITY_CODE", StringType, nullable = true),
      StructField("TAC", StringType, nullable = true),
      StructField("STL_SEQ_NO", StringType, nullable = true),
      StructField("LAST_UPD_TMS", StringType, nullable = true),
      StructField("ORDER_NO", StringType, nullable = true)
    )
  )
  val prop: Properties = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
  val url: String = DynamicConf.localhostUrl

  def batchClean(originFilePath: String, startDate: String, endDate: String, targetFilePath: String): Unit = {
    val dataFrame = sparkSession.read
      .option("sep", ",")
      .schema(afcOriginSchema)
      .csv(originFilePath)
    dataFrame.createOrReplaceTempView("afc_origin")
    val afcUse = "select TICKET_ID,TXN_DATE,TXN_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID from afc_origin"
    val afcFrame = sparkSession.sql(afcUse)
    afcFrame.persist()
    afcFrame.createOrReplaceTempView("afc_origin")
    sparkSession.udf.register("to_date_time", (date: String, time: String) => BatchCleanByStreaming.toDateTime(date, time))
    val startInt = startDate.toInt
    val cycleTime = endDate.toInt - startInt
    for (currentDate <- 0 to cycleTime) {
      val endInt = startInt + currentDate
      val date = endInt.toString
      val preNeedSql = s"select trim(TICKET_ID) TICKET_ID,to_date_time(TXN_DATE,TXN_TIME) TXN_DATE_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID from afc_origin where TXN_DATE=$date"
      val matchBeforeFrame = sparkSession.sql(preNeedSql)
      matchBeforeFrame.createOrReplaceTempView("afc")
      val inStationSql = "select TICKET_ID,TXN_DATE_TIME,TICKET_TYPE,TXN_STATION_ID from afc where TRANS_CODE=21"
      val outStationSql = "select TICKET_ID,TXN_DATE_TIME,TICKET_TYPE,TXN_STATION_ID from afc where TRANS_CODE=22"
      val inStationFrame = sparkSession.sql(inStationSql)
      val outStationFrame = sparkSession.sql(outStationSql)
      inStationFrame.createOrReplaceTempView("in_station")
      outStationFrame.createOrReplaceTempView("out_station")
      val stationMatchedToODSql =
        "select TICKET_ID,TICKET_TYPE,in_number,in_time,out_number,out_time from " +
          "(select * from " +
          "(select in_station.TICKET_ID,in_station.TICKET_TYPE,in_station.TXN_STATION_ID in_number,in_station.TXN_DATE_TIME in_time," +
          "out_station.TXN_STATION_ID out_number,out_station.TXN_DATE_TIME out_time," +
          "row_number() over(partition by in_station.TICKET_ID,in_station.TICKET_TYPE,in_station.TXN_STATION_ID,in_station.TXN_DATE_TIME sort by in_station.TICKET_ID) as row_num " +
          "from in_station join out_station " +
          "on in_station.TICKET_ID=out_station.TICKET_ID and in_station.TXN_DATE_TIME<out_station.TXN_DATE_TIME) as od) where row_num=1"
      val odMatchedFrame = sparkSession.sql(stationMatchedToODSql)
      val tableName = startInt / 100
      odMatchedFrame.write.mode(SaveMode.Append).jdbc(url, s"OD_$tableName", prop)
    }
  }

  def batchAFC(originFilePath: String, startDate: String): Unit = {
    val afcOriginFrame = sparkSession.read.schema(afcOriginSchema).csv(originFilePath)
    afcOriginFrame.createOrReplaceTempView("afc_origin")
    val afcUse = "select TICKET_ID,TXN_DATE,TXN_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID from afc_origin"
    sparkSession.sql(afcUse).createOrReplaceTempView("afc_origin")
    sparkSession.udf.register("to_date_time", (date: String, time: String) => BatchCleanByStreaming.toDateTime(date, time))
    val toDateTimeSql = "select TICKET_ID,to_date_time(TXN_DATE,TXN_TIME) TXN_DATE_TIME,TRANS_CODE,TXN_STATION_ID from afc_origin"
    sparkSession.sql(toDateTimeSql).createOrReplaceTempView("afc_origin")
    val queryInStation = "select TICKET_ID,TXN_DATE_TIME,'进站' TRANS_EVENT,TXN_STATION_ID TRANS_CODE from afc_origin where TRANS_CODE=21"
    val queryOutStation = "select TICKET_ID,TXN_DATE_TIME,'出站' TRANS_EVENT,TXN_STATION_ID TRANS_CODE from afc_origin where TRANS_CODE=22"
    sparkSession.sql(queryInStation).write.mode(SaveMode.Append).jdbc(url, s"AFC_${startDate.substring(0, 7)}", prop)
    sparkSession.sql(queryOutStation).write.mode(SaveMode.Append).jdbc(url, s"AFC_${startDate.substring(0, 7)}", prop)
  }
}

object BatchClean {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("BatchClean").master("local[*]").getOrCreate()
    val batchClean = new BatchClean(sparkSession)
    val originFilePath = "G:/Destop/轨道交通项目开发/各类数据/20200101test.csv"
    val startDate = "20200101"
    val endDate = "20200102"
    val targetFilePath = "G:/Destop/"
    //    batchClean.batchClean(originFilePath, startDate, endDate, targetFilePath)
    batchClean.batchAFC(originFilePath, startDate)

  }
}
