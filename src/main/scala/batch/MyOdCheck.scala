package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class MyOdCheck {
  private var afcPath: String = _
  private var date: String = _
  private var odPath: String = _
  private var startTime: String = _
  private var endTime: String = _

  def this(afcPath: String, date: String, odPath: String) {
    this()
    this.afcPath = afcPath
    this.date = date
    this.odPath = odPath
  }

  def this(afcPath: String, date: String, odPath: String, startTime: String, endTime: String) {
    this()
    this.afcPath = afcPath
    this.date = date
    this.odPath = odPath
    this.startTime = startTime
    this.endTime = endTime
  }

  def check(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("checkOd")
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
      .csv(afcPath)
    dataFrame.createOrReplaceTempView("afc")
    val selectData = s"select $date,count(*) from afc where TXN_DATE=$date"
    sparkSession.sql(selectData).show()
    val odFrame = sparkSession.read
      .option("sep", ",")
      .csv(odPath)
    odFrame.createOrReplaceTempView("od")
    val selectOd = s"select $date,count(*) from od"
    sparkSession.sql(selectOd).show()
    sparkSession.stop()
  }

  def getNotMatchOd(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("getNotMatchOd")
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
      .csv(afcPath)
    dataFrame.createOrReplaceTempView("afc")
    val selectOneDay = s"select TXN_STATION_ID,TRANS_CODE from afc where TXN_DATE=$date and TXN_TIME >=$startTime and TXN_TIME <=$endTime"
    val oneDayAfc = sparkSession.sql(selectOneDay).cache()
    oneDayAfc.createOrReplaceTempView("afc")
    val inStationSql = "select count(*) in_sum from afc where TRANS_CODE=21"
    val inStationFrame = sparkSession.sql(inStationSql)
    inStationFrame
      .createOrReplaceTempView("in_station")
    val outStationSql = "select count(*) out_sum from afc where TRANS_CODE=22"
    val outStationFrame = sparkSession.sql(outStationSql)
    outStationFrame
      .createOrReplaceTempView("out_station")
    val odSchema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("kind", IntegerType, nullable = true),
        StructField("in_number", IntegerType, nullable = true),
        StructField("in_time", StringType, nullable = true),
        StructField("out_number", IntegerType, nullable = true),
        StructField("out_time", StringType, nullable = true)
      )
    )
    val odFrame = sparkSession.read
      .option("sep", ",")
      .schema(odSchema)
      .csv(odPath)
    odFrame.createOrReplaceTempView("od")

    val selectOd = "select count(*) od_num from od where in_time>=(2021-02-24 00:00:00) and in_time<= (2021-02-24 06:00:00)"
    val odDataFrame = sparkSession.sql(selectOd)
    odDataFrame
      .createOrReplaceTempView("od")
    inStationFrame.show()
    outStationFrame.show()
    odDataFrame.show()
    sparkSession.stop()
  }
}

object MyOdCheck {
  def main(args: Array[String]): Unit = {
    val afcPath = args(0)
    val date = args(1)
    val startTime = args(2)
    val endTime = args(3)
    val odPath = args(4)
    val checkOd = new MyOdCheck(afcPath = afcPath, date = date, startTime = startTime, endTime = endTime, odPath = odPath)
    //    checkOd.check()
    checkOd.getNotMatchOd()
  }
}
