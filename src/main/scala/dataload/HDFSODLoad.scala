package dataload

import dataload.base.OracleChongQingLoad
import flowdistribute.OdWithTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class HDFSODLoad(hdfsPath: String) {
  private val schema: StructType = StructType(
    Array(
      StructField("id", IntegerType, nullable = true),
      StructField("in_number", IntegerType, nullable = true),
      StructField("out_number", IntegerType, nullable = true),
      StructField("in_time", StringType, nullable = true),
      StructField("passengers", DoubleType, nullable = true)
    )
  )

  def getOdRdd(sparkSession: SparkSession): RDD[OdWithTime] = {
    val chongQingLoad = new OracleChongQingLoad()
    val mapIdFrame = chongQingLoad.load()
    mapIdFrame.createOrReplaceTempView("od_transfer")
    val odFrame = sparkSession.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .schema(schema)
      .load(hdfsPath)
    odFrame.createOrReplaceTempView("od")
    val odTransferIdSql = "select target1.CZ_ID in_number,in_time,target2.CZ_ID out_number,passengers " +
      "from od origin join od_transfer target1 on origin.in_number=target1.AFC_ID " +
      "join od_transfer target2 on origin.out_number=target2.AFC_ID where target1.CZ_ID<>target2.CZ_ID"
    val odTransferIdFrame = sparkSession.sql(odTransferIdSql)
    val odWithTimeRdd: RDD[OdWithTime] = odTransferIdFrame.rdd.map(od => {
      val inId = od.getDecimal(0).intValue().toString
      val inTime = od.getString(1)
      val outId = od.getDecimal(2).intValue().toString
      val passengers = od.getDouble(3)
      val odWithTime = new OdWithTime(inId, outId, inTime, passengers)
      odWithTime
    }).filter(x => x.getPassengers > 0)
    odWithTimeRdd
  }
}

object HDFSODLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val path = "G:/Destop/OD.csv"
    val odLoad = new HDFSODLoad(path)
    odLoad.getOdRdd(sparkSession).foreach(x => println(s"数据为$x"))
  }
}
