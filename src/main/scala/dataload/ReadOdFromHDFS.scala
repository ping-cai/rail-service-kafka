package dataload

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ReadOdFromHDFS(sparkSession: SparkSession) extends ReadFrom[HDFSParam] {
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
      StructField("TRANS_CODE", IntegerType, nullable = true),
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

  override def read(param: HDFSParam): DataFrame = {
    val originFilePath = param.originFilePath
    val dataFrame = sparkSession.read
      .option("sep", ",")
      .schema(afcOriginSchema)
      .csv(originFilePath)
    dataFrame.createOrReplaceTempView("afc_origin")
    val afcUse = "select TICKET_ID,TXN_DATE,TXN_TIME,TICKET_TYPE,TRANS_CODE,TXN_STATION_ID from afc_origin"
    val afcFrame = sparkSession.sql(afcUse)
    afcFrame
  }
}
