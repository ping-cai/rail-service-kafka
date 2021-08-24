package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ODAddStationName {
  private var originFilePath: String = _
  private var startDate: String = _
  private var endDate: String = _
  private var targetFilePath: String = _
  private var stationNameFilePath: String = _

  def this(originFilePath: String, startDate: String, endDate: String, targetFilePath: String, stationNameFilePath: String) {
    this()
    this.originFilePath = originFilePath
    this.startDate = startDate
    this.endDate = endDate
    this.targetFilePath = targetFilePath
    this.stationNameFilePath = stationNameFilePath
  }

  def odAddStationName(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("scala.ODAddStationName")
      .getOrCreate()

    val stationInfoFrame = sparkSession.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true") //自动推断列类型
      .csv(stationNameFilePath)
    val idNameFrame = stationInfoFrame.select("STATIONID", "CZNAME")
    idNameFrame.persist()
    idNameFrame.createOrReplaceTempView("station_id_name")
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
    val startInt = startDate.toInt
    val cycleTime = endDate.toInt - startInt
    for (i <- 0 to cycleTime) {
      val tempInt = startInt + i
      val tempFileName = s"od-$tempInt-result.csv"
      val dataFrame = sparkSession.read
        .option("sep", ",")
        .schema(schema)
        .csv(originFilePath + tempFileName)
      dataFrame.createOrReplaceTempView("od")
      val odAddStationNameSql = "select id,kind,in_number,target1.CZNAME in_name,in_time,out_number,target2.CZNAME out_name,out_time " +
        "from od join station_id_name target1 on od.in_number=target1.STATIONID " +
        "join station_id_name target2 on od.out_number=target2.STATIONID "
      val odWithNameFrame = sparkSession.sql(odAddStationNameSql)
      val resultFileName = s"odHasName-$tempInt-result.csv"
      odWithNameFrame.write.mode("append").csv(targetFilePath + resultFileName)
    }
    sparkSession.stop()
  }
}

object ODAddStationName {
  def main(args: Array[String]): Unit = {
    //    val originFilePath = "hdfs://hacluster/afcdata-2020/od-data-2020/"
    //    val startDate = "20200101"
    //    val endDate = "20200131"
    //    val targetFilePath = "hdfs://hacluster/afcdata-2020/od-data-2020-aggregation/"
    //    val stationNameFilePath = "hdfs://hacluster/oracle/scott/chongqing_stations_nm.csv"
    val originFilePath = args(0)
    val startDate = args(1)
    val endDate = args(2)
    val targetFilePath = args(3)
    val stationNameFilePath = args(4)
    val addStationName = new ODAddStationName(originFilePath, startDate, endDate, targetFilePath, stationNameFilePath)
    addStationName.odAddStationName()
  }
}
