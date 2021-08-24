package batch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ODTimeDivision {
  private var originFilePath: String = _
  private var startDate: String = _
  private var endDate: String = _
  private var targetFilePath: String = _
  private var divisionSize: Int = _

  def this(originFilePath: String, startDate: String, endDate: String, targetFilePath: String, divisionSize: Int) {
    this()
    this.originFilePath = originFilePath
    this.startDate = startDate
    this.endDate = endDate
    this.targetFilePath = targetFilePath
    this.divisionSize = divisionSize
  }

  def odLoadToDivision(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ODTimeDivision")
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
    val startInt = startDate.toInt
    val cycleTime = endDate.toInt - startInt
    sparkSession.udf.register("time_divide", (dateTime: String, divisionSize: Int) => ODTimeDivision.timeDivide(dateTime, divisionSize))
    for (i <- 0 to cycleTime) {
      val tempInt = startInt + i
      val tempFilePath = s"od-$tempInt-result.csv"
      val dataFrame = sparkSession.read
        .schema(schema)
        .option("sep", ",")
        .csv(originFilePath + tempFilePath)
      dataFrame.createOrReplaceTempView("od")
      val odTimeDivisionAggregationSql = "select in_number,start_time,out_number,end_time,count(*) passengers from " +
        s"(select in_number,time_divide(in_time,$divisionSize) start_time,out_number,time_divide(out_time,$divisionSize) end_time from od) od_division " +
        "group by in_number,start_time,out_number,end_time "
      val AggregationOd = sparkSession.sql(odTimeDivisionAggregationSql)
      val resultFilePath = s"od-$tempInt-Aggregation-$divisionSize.csv"
      AggregationOd.write.mode("append").csv(targetFilePath + resultFilePath)
    }
    sparkSession.stop()
  }
}

object ODTimeDivision {
  def main(args: Array[String]): Unit = {
    //    val originFilePath = "hdfs://hacluster/afcdata-2020/od-data-2020/"
    //    val startDate = "20200101"
    //    val endDate = "20200131"
    //    val targetFilePath = "hdfs://hacluster/afcdata-2020/od-data-2020-aggregation/"
    //    val divisionSize = 15
    val originFilePath = args(0)
    val startDate = args(1)
    val endDate = args(2)
    val targetFilePath = args(3)
    val divisionSize = args(4).toInt
    val timeDivision = new ODTimeDivision(originFilePath, startDate, endDate, targetFilePath, divisionSize)
    timeDivision.odLoadToDivision()
  }

  def timeDivide(dateTime: String, divisionSize: Int): String = {
    val dateAndTime = dateTime.split(" ")
    val date = dateAndTime(0)
    val time = dateAndTime(1)
    val timeArray = time.split(":")
    val hour = timeArray(0)
    val minute = timeArray(1).toInt
    val divisionBlock = minute / divisionSize
    if (divisionBlock == 0) {
      val timeDivided = s"$date $hour:00:00"
      timeDivided
    } else {
      val finalMinute = divisionBlock * divisionSize
      val timeDivided = s"$date $hour:$finalMinute:00"
      timeDivided
    }
  }

}