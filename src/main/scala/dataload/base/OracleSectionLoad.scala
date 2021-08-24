package dataload.base

import conf.DynamicConf
import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleSectionLoad extends Load {
  private var sectionTable: String = DynamicConf.sectionTable
  private var url: String = Load.url

  def this(sectionTable: String) {
    this()
    this.sectionTable = sectionTable
  }

  def this(sectionTable: String, url: String) {
    this()
    this.sectionTable = sectionTable
    this.url = url
  }

  /**
    * 获得路网图数据
    *
    * @return DataFrame "QJ_ID", "CZ1_ID", "CZ2_ID", "QJ_SXX", "QJ_LENGTH", "QJ_LJM"
    */
  override def load(): DataFrame
  = {
    val sparkSession = SparkSession.builder()
      .appName("OracleSectionLoad")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val dataFrame = sparkSession.read.jdbc(url, sectionTable, prop)
    dataFrame.createOrReplaceTempView("yxtSection")
    val filterSql = s"SELECT * FROM yxtSection WHERE CZ1_NAME not LIKE '%车辆%' AND CZ1_NAME NOT LIKE '%停车场%' AND CZ1_NAME NOT LIKE '%线路所%' AND CZ2_NAME not LIKE '%车辆段%' AND CZ2_NAME NOT LIKE '%停车场%' AND CZ2_NAME NOT LIKE '%线路所%' AND QJ_LJM<>'成都'"
    val filterFrame = sparkSession.sql(filterSql)
    val sectionFrame = filterFrame.select("QJ_ID", "CZ1_ID", "CZ2_ID", "QJ_SXX", "QJ_LENGTH", "QJ_LJM", "CZ1_NAME", "CZ2_NAME")
    sectionFrame
  }

}

object OracleSectionLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sectionLoad = new OracleSectionLoad()
    val sectionFrame = sectionLoad.load()
    sectionFrame.show(1000)
  }
}
