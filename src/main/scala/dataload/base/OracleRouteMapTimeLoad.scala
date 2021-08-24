package dataload.base

import java.util
import java.util.Properties

import conf.DynamicConf
import costcompute.{SectionTravelTime, TravelTime}
import dataload.Load
import domain.dto.SectionTravelToOracle
import domain.{Section, SectionTravelSecondWithRate}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class OracleRouteMapTimeLoad extends Load {
  private var sectionTravelTable: String = DynamicConf.sectionTravelTable
  private var url: String = Load.url

  def this(sectionTravelTable: String, url: String) {
    this()
    this.sectionTravelTable = sectionTravelTable
    this.url = url
  }

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("OracleSectionTravelTimeLoad")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val passengerFreightTime = sparkSession.read.jdbc(url, sectionTravelTable, prop)
    passengerFreightTime.createOrReplaceTempView("yxtKhsk")
    val sectionTravelTimeSelect = "select cast(origin.CZ_ID as int) CZ1_ID,origin.CZ_NAME CZ1_NAME,origin.DEP_TIME," +
      "cast(target.CZ_ID as int) CZ2_ID,target.CZ_NAME CZ2_NAME,target.ARR_TIME " +
      "FROM yxtKhsk origin " +
      "join yxtKhsk target on origin.LCXH=target.LCXH AND origin.XH=target.XH -1 " +
      "GROUP BY origin.CZ_ID,origin.CZ_NAME,origin.DEP_TIME,target.CZ_ID,target.CZ_NAME,target.ARR_TIME,origin.LCXH,origin.XH " +
      "ORDER BY origin.LCXH,origin.XH"
    val sectionTravelTimeFrame = sparkSession.sql(sectionTravelTimeSelect)
    sectionTravelTimeFrame
  }
}

object OracleRouteMapTimeLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val travelTimeLoad = new OracleRouteMapTimeLoad
    val travelTimeLoadFrame = travelTimeLoad.load()
    travelTimeLoadFrame.createTempView("section_time")
    val sectionTravelTimes = getSectionTravelTimeList(travelTimeLoadFrame)
    val sectionTravelAvgTime = new util.HashMap[Section, util.List[Int]]()
    sectionTravelTimes.forEach(x => {
      val section = x.getSection
      val travelTime = x.getTravelTime
      val seconds = TravelTime.getSeconds(travelTime.getDepartureTime, travelTime.getArrivalTime)
      if (sectionTravelAvgTime.containsKey(section)) {
        val secondsTime = sectionTravelAvgTime.get(section)
        secondsTime.add(seconds)
      } else {
        val secondList = new util.ArrayList[Int]()
        secondList.add(seconds)
        sectionTravelAvgTime.put(section, secondList)
      }
    })
    val secondWithRates = new util.ArrayList[SectionTravelSecondWithRate]()
    sectionTravelAvgTime.forEach((x, y) => {
      y.sort((num1, num2) => num1 - num2)
      secondWithRates.add(new SectionTravelSecondWithRate(x, y.get(y.size() / 2), 1.0))
    })
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val sectionWithRatesScala = secondWithRates.asScala
    val sectionWithRatesRdd: RDD[SectionTravelSecondWithRate] = sc.makeRDD(sectionWithRatesScala)
    val sectionWithRateFrame = sectionWithRatesRdd.map(x => SectionTravelToOracle(x.getSection.getInId, x.getSection.getOutId, x.getTimeSeconds, x.getRate)).toDF
    val prop = new Properties()
    prop.put("user", "scott")
    prop.put("password", "tiger")
    val oracleSectionTravelTimeLoad = new OracleRouteMapTimeLoad
    sectionWithRateFrame.write.mode("append").jdbc(oracleSectionTravelTimeLoad.url, "SECTION_TRAVEL_TIME", prop)
  }

  def getSectionTravelTimeList(sectionTravelFrame: DataFrame): java.util.ArrayList[SectionTravelTime] = {
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

