package check

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class OdBackStep(sparkSession: SparkSession) extends Serializable {

  def getOdShare(odSharePath: String): DataFrame = {
    val schema: StructType = StructType(
      Array(
        StructField("timeKeyStart", StringType, nullable = true),
        StructField("timeKeyEnd", StringType, nullable = true),
        StructField("sectionId", IntegerType, nullable = true),
        StructField("startId", StringType, nullable = true),
        StructField("endId", StringType, nullable = true),
        StructField("origin", StringType, nullable = true),
        StructField("destination", StringType, nullable = true),
        StructField("inTime", StringType, nullable = true),
        StructField("outTime", StringType, nullable = true),
        StructField("odShareFlow", DecimalType(36, 18), nullable = true),
        StructField("odPassengers", DecimalType(36, 18), nullable = true)
      )
    )
    val odShareFrame = sparkSession.read.schema(schema).csv(odSharePath)
    odShareFrame
  }

  def getSectionOver(sectionOverPath: String): DataFrame = {
    val schema: StructType = StructType(
      Array(
        StructField("line_name", StringType, nullable = true),
        StructField("section_id", IntegerType, nullable = true),
        StructField("section_capacity", IntegerType, nullable = true),
        StructField("station_id1", IntegerType, nullable = true),
        StructField("station_name1", StringType, nullable = true),
        StructField("station_id2", IntegerType, nullable = true),
        StructField("station_name2", StringType, nullable = true),
        StructField("section_flow", DecimalType(36, 18), nullable = true),
        StructField("section_over_flow", DecimalType(36, 18), nullable = true),
        StructField("full_load_rate", StringType, nullable = true)
      )
    )
    val sectionOverFrame = sparkSession.read.schema(schema).csv(sectionOverPath)
    sectionOverFrame
  }

  def backStep(odShareFrame: DataFrame, sectionOverFrame: DataFrame, resultPath: String): Unit = {
    odShareFrame.createOrReplaceTempView("od_share")
    sectionOverFrame.createOrReplaceTempView("section_over")
    val backStepSql = "select line_name,section_id,section_capacity,station_id1,station_name1,station_id2,station_name2,section_flow,section_over_flow,full_load_rate," +
      "origin,destination,inTime,outTime,odShareFlow,odPassengers " +
      "from od_share " +
      "join section_over on " +
      "od_share.sectionId=section_over.section_id " +
      "group by line_name,section_id,section_capacity,station_id1,station_name1,station_id2,station_name2,section_flow,section_over_flow,full_load_rate," +
      "origin,destination,inTime,outTime,odShareFlow,odPassengers"
    val resultFrame = sparkSession.sql(backStepSql)
    resultFrame.write.mode("append").csv(resultPath)
  }


}

object OdBackStep {
  def main(args: Array[String]): Unit = {
    val hdfsPrefix = "hdfs://hacluster"
    val odSharePath = s"$hdfsPrefix/${args(0).split(":")(1)}"
    val sectionOverPath = s"$hdfsPrefix/${args(1).split(":")(1)}"
    val resultPath = s"$hdfsPrefix/${args(2).split(":")(1)}"
    val sparkSession = SparkSession.builder().appName("OdBackStep").getOrCreate()
    val odBackStep = new OdBackStep(sparkSession)
    startBackStep(odBackStep, odSharePath, sectionOverPath, resultPath)
  }

  def startBackStep(odBackStep: OdBackStep, odSharePath: String, sectionOverPath: String, resultPath: String): Unit = {
    val odShareFrame = odBackStep.getOdShare(odSharePath)
    val sectionOverFrame = odBackStep.getSectionOver(sectionOverPath)
    odBackStep.backStep(odShareFrame, sectionOverFrame, resultPath)
  }
}