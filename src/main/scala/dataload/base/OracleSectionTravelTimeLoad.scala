package dataload.base

import java.util

import conf.DynamicConf
import costcompute.{SectionTravelTime, TravelTime}
import dataload.Load
import domain.Section
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleSectionTravelTimeLoad extends Load {
  private var sectionTravelTable: String = DynamicConf.sectionTravelTable
  private var url: String = Load.url

  def this(sectionTravelTable: String, url: String) {
    this()
    this.sectionTravelTable
    this.url = url
  }

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("OracleSectionTravelTimeLoad")
      .getOrCreate()
    val passengerFreightTime = sparkSession.read.jdbc(url, sectionTravelTable, prop)
    passengerFreightTime.createOrReplaceTempView("yxtKhsk")
    val sectionTravelTimeSelect = "select cast(origin.CZ_ID as int) CZ1_ID,origin.CZ_NAME CZ1_NAME,origin.DEP_TIME," +
      "cast(target.CZ_ID as int) CZ2_ID,target.CZ_NAME CZ2_NAME,target.ARR_TIME " +
      "FROM yxtKhsk origin " +
      "join yxtKhsk target on origin.LCXH=target.LCXH AND origin.XH=target.XH-1 " +
      "GROUP BY origin.CZ_ID,origin.CZ_NAME,origin.DEP_TIME,target.CZ_ID,target.CZ_NAME,target.ARR_TIME,origin.LCXH,origin.XH " +
      "ORDER BY origin.LCXH,origin.XH"
    val sectionTravelTimeFrame = sparkSession.sql(sectionTravelTimeSelect)
    sectionTravelTimeFrame
  }

  def getSectionTravelTimeList(sectionTravelFrame: DataFrame): java.util.List[SectionTravelTime] = {
    val sectionTravelTimes = new util.ArrayList[SectionTravelTime]()
    sectionTravelFrame.rdd.collect.map(x => {
      val inId = x.getInt(0).toString
      val departureTime = x.getString(2)
      val outId = x.getInt(3).toString
      val arrivalTime = x.getString(5)
      val section = new Section(inId, outId)
      val travelTime = new TravelTime(departureTime, arrivalTime)
      val sectionTravelTime = new SectionTravelTime(section, travelTime)
      sectionTravelTimes.add(sectionTravelTime)
    })
    sectionTravelTimes
  }
}

object OracleSectionTravelTimeLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val travelTimeLoad = new OracleSectionTravelTimeLoad
    val travelTimeLoadFrame = travelTimeLoad.load()
    travelTimeLoadFrame.createTempView("section_time")
  }

}

