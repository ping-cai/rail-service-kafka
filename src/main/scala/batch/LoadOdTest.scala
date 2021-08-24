package batch

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class LoadOdTest {
  private var odData: String = _
  private var url: String = "jdbc:oracle:thin:@192.168.1.17:1521:ORCL"
  private var user = "scott"
  private var password = "tiger"
  private var targetTable: String = _

  def this(odData: String, targetTable: String) {
    this()
    this.odData = odData
    this.targetTable = targetTable
  }
}

object LoadOdTest {
  def main(args: Array[String]): Unit = {
    val odData = args(0)
    val targetTable = args(1)
    val loadOdTest = new LoadOdTest(odData, targetTable)
    val sparkSession = SparkSession.builder().
      appName("LoadOdTest").getOrCreate()
    val schema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("in_number", IntegerType, nullable = true),
        StructField("in_time", StringType, nullable = true),
        StructField("out_number", IntegerType, nullable = true),
        StructField("passengers", IntegerType, nullable = true),
      )
    )
    val odFrame = sparkSession.read.schema(schema).csv(loadOdTest.odData)
    odFrame.createOrReplaceTempView("od")
    val distinctStation = "select in_number from od group by in_number order by in_number"
    val stationFrame = sparkSession.sql(distinctStation)
    val prop = new Properties()
    prop.put("user", loadOdTest.user)
    prop.put("password", loadOdTest.password)
    stationFrame.write.mode("append").jdbc(loadOdTest.url, loadOdTest.targetTable, prop)
    sparkSession.stop()
  }
}
