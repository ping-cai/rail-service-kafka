package batch

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class ODAggregationToMysql {
  private var url: String = "jdbc:mysql://localhost:3306/passenger_flow_data?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"
  private var originFilePath: String = _
  private var startDate: String = _
  private var endDate: String = _
  private var targetTable: String = _
  private var divisionSize: Int = _

  def this(url: String, originFilePath: String, startDate: String, endDate: String, targetTable: String, divisionSize: Int) {
    this()
    this.url = url
    this.originFilePath = originFilePath
    this.startDate = startDate
    this.endDate = endDate
    this.targetTable = targetTable
    this.divisionSize = divisionSize
  }

  def save(): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ODAggregationToMysql")
      .master("local[*]")
      .getOrCreate()
    val schema: StructType = StructType(
      Array(
        StructField("in_number", IntegerType, nullable = true),
        StructField("in_time", StringType, nullable = true),
        StructField("out_number", IntegerType, nullable = true),
        StructField("out_time", StringType, nullable = true),
        StructField("passengers", DoubleType, nullable = true)
      )
    )
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "960064")
    val startInt = startDate.toInt
    val cycleTime = endDate.toInt - startInt
    for (i <- 0 to  cycleTime) {
      val fileSequence = startInt + i
      val originFileName = s"od-$fileSequence-Aggregation-$divisionSize.csv"
      val dataFrame = sparkSession.read.schema(schema)
        .csv(originFilePath + originFileName)
      dataFrame.write.mode("append").jdbc(url, targetTable, prop)
    }
    sparkSession.stop()
  }

}

object ODAggregationToMysql {

  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://localhost:3306/passenger_flow_data?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"
    val originFilePath = "G:/Destop/od-data-2020-aggregation/"
    val startDate = "20200101"
    val endDate = "20200131"
    val targetTable = "od_aggregation_15"
    val divisionSize = 15
    val oDAggregationToMysql = new ODAggregationToMysql(url, originFilePath, startDate, endDate, targetTable, divisionSize)
    oDAggregationToMysql.save()
  }
}
