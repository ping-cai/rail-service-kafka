package dataload.base

import conf.DynamicConf
import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleChongQingLoad() extends Load {
  private var afcStationTable: String = DynamicConf.afcStationTable
  private var stationTable: String = DynamicConf.stationTable
  private var url: String = Load.url

  def this(stationTable: String, url: String) {
    this()
    this.afcStationTable = stationTable
    this.url = url
  }

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("OracleCHONGQINGLoad")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val afcFrame = sparkSession.read.jdbc(url, afcStationTable, prop)
    afcFrame.createOrReplaceTempView("chongqing_stations_nm")
    val stationFrame = sparkSession.read.jdbc(url, stationTable, prop)
    stationFrame.createOrReplaceTempView("yxtStation")
    val mapIdSql = "SELECT origin.STATIONID AFC_ID,target.CZ_ID FROM chongqing_stations_nm origin JOIN yxtStation target on origin.CZNAME=target.CZ_NAME AND origin.LINENAME=target.LJM"
    val afcMapFrame = sparkSession.sql(mapIdSql)
    afcMapFrame
  }
}

object OracleChongQingLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val chongQingLoad = new OracleChongQingLoad()
    val afcFrame = chongQingLoad.load()
    afcFrame.show(1000)
  }
}