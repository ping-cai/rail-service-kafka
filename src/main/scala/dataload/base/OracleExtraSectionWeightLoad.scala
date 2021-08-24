package dataload.base

import conf.DynamicConf
import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleExtraSectionWeightLoad extends Load {
  private var sectionExtraTable = DynamicConf.sectionExtraTable
  private var url: String = Load.url

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder().appName("OracleExtraSectionWeightLoad").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val dataFrame = sparkSession.read.jdbc(url, sectionExtraTable, prop)
    dataFrame
  }
}

object OracleExtraSectionWeightLoad{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val sectionWeightLoad = new OracleExtraSectionWeightLoad
    sectionWeightLoad.load().show()
  }
}
