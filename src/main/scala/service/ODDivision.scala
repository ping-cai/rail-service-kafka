package service

import dataload.Load
import org.apache.spark.sql.DataFrame

class ODDivision extends DivideTime {
  //  interval 时间粒度
  private var interval: Int = _

  def this(interval: Int) {
    this()
    this.interval = interval
  }

  /**
    *
    * @param data StructField("id", StringType, nullable = true),
    *             StructField("kind", IntegerType, nullable = true),
    *             StructField("in_number", IntegerType, nullable = true),
    *             StructField("in_time", TimestampType, nullable = true),
    *             StructField("out_number", IntegerType, nullable = true),
    *             StructField("out_time", TimestampType, nullable = true)
    * @return 划分了时段的OD结果集
    */
  override def divide(data: DataFrame): DataFrame = {
    data.createOrReplaceTempView("od")
    val divideTime = interval * 60
    val sparkSession = data.sparkSession
    val divideSql = s"select in_number,cast(round(cast(in_time as bigint)/$divideTime)*$divideTime as timestamp) in_time," +
      s"out_number,cast(round(cast(out_time as bigint)/$divideTime)*$divideTime as timestamp) out_time " +
      s"from od"
    val divideTimeFrame = sparkSession.sql(divideSql)
    divideTimeFrame.createOrReplaceTempView("od")
    val groupPassenger = "select *,count(*) passengers from od group by in_number,in_time,out_number,out_time"
    val frameWithPassenger = sparkSession.sql(groupPassenger)
    frameWithPassenger
  }
}

object ODDivision {
  /**
    *
    * @param chongQingLoad 表名chongqing_stations_nm fields:stationid,type,czname,pyname,linename
    * @param stationLoad   表名2-yxtStation fields: cz_id,cz_name,ljm
    * @param od            转换原始OD的站名id为路网图进行K短路计算的Id
    * @return
    */
  def transferOdId(chongQingLoad: Load, stationLoad: Load, od: DataFrame): DataFrame = {
    val chongQingData = chongQingLoad.load()
    val stationData = stationLoad.load()
    val sparkSession = od.sparkSession
    stationData.createOrReplaceTempView("station")
    chongQingData.createOrReplaceTempView("chongqing_stations_nm")
    od.createOrReplaceTempView("od")
    val equalTransfer = "select origin.CZ_ID,target.STATIONID FROM station origin join " +
      "chongqing_stations_nm target on origin.CZ_NAME=target.CZNAME " +
      "and origin.LJM=target.LINENAME"
    val equalIdTable = sparkSession.sql(equalTransfer)
    equalIdTable.createOrReplaceTempView("transfer_id")
    val transferEndOdSql = "select target1.CZ_ID in_time,target2.CZ_ID,out_time,passengers " +
      "from od origin " +
      "join transfer_id target1 on origin.in_number=target1.CZ_ID " +
      "join transfer_id target2 on origin.in_number=target2.CZ_ID "
    val transferEndOd = sparkSession.sql(transferEndOdSql)
    transferEndOd
  }

  /**
    * 获取指定时间段的OD数据
    *
    * @param startTime 指定的开始时间
    * @param odData    OD数据DataFrame
    * @return 指定时间段的OD数据
    */
  def getSpecifiedTimeData(startTime: String, odData: DataFrame): DataFrame = {
    val sparkSession = odData.sparkSession
    odData.createOrReplaceTempView("od")
    val specifiedSql = s"select * from od where timestamp(in_time)=timestamp('$startTime')"
    val specifiedData = sparkSession.sql(specifiedSql)
    specifiedData
  }
}
