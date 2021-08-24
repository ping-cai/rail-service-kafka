package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import tools.AFCDateClean

class DataClean {
  private var originFilePath: String = _
  private var startDate: String = _
  private var endDate: String = _
  private var targetFilePath: String = _

  def this(originFilePath: String, startDate: String, endDate: String, targetFilePath: String) {
    this()
    this.originFilePath = originFilePath
    this.startDate = startDate
    this.endDate = endDate
    this.targetFilePath = targetFilePath
  }

  def afc2020Clean(): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("DataClean")
      .getOrCreate()
    val schema: StructType = StructType(
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
    val dataFrame = sparkSession.read
      .option("sep", ",")
      .schema(schema)
      .csv(originFilePath)
    dataFrame.createOrReplaceTempView("afc_origin")
    val afcUse = "select TICKET_ID,TXN_DATE,TXN_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID from afc_origin"
    val afcFrame = sparkSession.sql(afcUse)
    afcFrame.persist()
    afcFrame.createOrReplaceTempView("afc_origin")
    sparkSession.udf.register("to_date_time", (date: String, time: String) => DataClean.toDateTime(date, time))
    val startInt = startDate.toInt
    val cycleTime = endDate.toInt - startInt
    for (i <- 0 to cycleTime) {
      val endInt = startInt + i
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
      val resultFilePath = s"od-$endInt-result.csv"
      odMatchedFrame.write.mode("append").csv(targetFilePath + resultFilePath)
    }
    sparkSession.stop()
  }
}

object DataClean {
  def main(args: Array[String]): Unit = {
    val originFilePath = args(0)
    val startDate = args(1)
    val endDate = args(2)
    val targetFilePath = args(3)
    //    val originFilePath = "hdfs://hacluster/afcdata-2020/20200101.csv"
    //    val startDate = "20200101"
    //    val endDate = "20200131"
    //    val targetFilePath = "hdfs://hacluster/afcdata-2020/od-data-2020/"

    val dataClean = new DataClean(originFilePath, startDate, endDate, targetFilePath)
    dataClean.afc2020Clean()
  }

  def od2020Load(targetFilePath: String, queryData: String): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("DataClean")
      .master("local[*]")
      .getOrCreate()
    val schema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("kind", IntegerType, nullable = true),
        StructField("in_number", IntegerType, nullable = true),
        StructField("in_time", StringType, nullable = true),
        StructField("out_number", IntegerType, nullable = true),
        StructField("out_time", StringType, nullable = true)
      )
    )
    val dataFrame = sparkSession.read
      .option("sep", ",")
      .schema(schema)
      //      .option("inferSchema", "true")//自动推断列类型
      .csv(targetFilePath)
    dataFrame.createOrReplaceTempView("od")
    val queryOneDay = s"select * from od where date_format(in_time,'yyyyMMdd')=$queryData"
    sparkSession.sql(queryOneDay).show()
  }

  def afc2018Clean(): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("DataClean")
      .master("local[*]")
      .getOrCreate()
    val schema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("days", IntegerType, nullable = true),
        StructField("shijian", IntegerType, nullable = true),
        StructField("kind", StringType, nullable = true),
        StructField("action", StringType, nullable = true),
        StructField("stations", IntegerType, nullable = true),
        StructField("beforebanlance", DoubleType, nullable = true),
        StructField("money", DoubleType, nullable = true),
        StructField("afterbanlance", DoubleType, nullable = true),
        StructField("liancheng", StringType, nullable = true),
        StructField("counter", IntegerType, nullable = true),
        StructField("trainid", IntegerType, nullable = true),
        StructField("recivetime", StringType, nullable = true)
      )
    )
    val dataFrame = sparkSession.read
      .schema(schema)
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("G:/Destop/轨道交通项目开发/开发阶段/各类数据/AFC原始数据.txt")
    dataFrame.createOrReplaceTempView("afc")
    val aFCDateClean = new AFCDateClean
    sparkSession.udf.register("standardDateConversion", (dateString: String) => aFCDateClean.standardDateConversion(dateString))
    val cleanSql = "select trim(id) id,kind,action,stations,beforebanlance,money,afterbanlance,standardDateConversion(recivetime) recivetime from afc"
    val cleanedData = sparkSession.sql(cleanSql)
    cleanedData.createOrReplaceTempView("afc_cleaned")
    val year = 2018
    val month = 9
    val inStation = s"select * from afc_cleaned where action='进站' and year(recivetime)=$year and month(recivetime) = $month"
    val outStation = s"select * from afc_cleaned where action='出站' and year(recivetime)=$year and month(recivetime) = $month"
    val inStationData = sparkSession.sql(inStation)
    inStationData.createOrReplaceTempView("in_station")
    val outStationData = sparkSession.sql(outStation)
    outStationData.createOrReplaceTempView("out_station")
    val stationMatchedToODSql =
      "select od.*,row_number() over(partition by id,kind,in_number,in_time sort by id) as row_num from " +
        "(select in_station.id,in_station.kind,in_station.stations in_number,in_station.recivetime in_time," +
        "out_station.stations out_number,out_station.recivetime out_time " +
        "from in_station join out_station " +
        "on in_station.id=out_station.id and in_station.recivetime<out_station.recivetime) as od "
    val stationMatchedToODSql2 =
      "select * from " +
        "(select in_station.id,in_station.kind,in_station.stations in_number,in_station.recivetime in_time," +
        "out_station.stations out_number,out_station.recivetime out_time," +
        "row_number() over(partition by in_station.id,in_station.kind,in_station.stations,in_station.recivetime sort by in_station.id) as row_num " +
        "from in_station join out_station " +
        "on in_station.id=out_station.id and in_station.recivetime<out_station.recivetime) as od "
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
