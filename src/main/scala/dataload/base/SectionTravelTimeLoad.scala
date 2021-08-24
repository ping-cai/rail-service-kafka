package dataload.base

import java.util

import dataload.Load
import domain.{Section, TravelTimeAndRate}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需要在数据库中提前设置好这张表
  * 加载区间运行时间的中位数以及惩罚系数的
  */
class SectionTravelTimeLoad extends Load {
  private var sectionTravelTimeTable: String = "SECTION_TRAVEL_TIME"
  private var url: String = Load.url

  def this(sectionTravelTimeTable: String, url: String) {
    this()
    this.sectionTravelTimeTable = sectionTravelTimeTable
    this.url = url
  }

  def this(sectionTravelTimeTable: String) {
    this()
    this.sectionTravelTimeTable = sectionTravelTimeTable
  }

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("SectionTravelTimeLoad")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sectionTravelTimeFrame = sparkSession.read.jdbc(url, sectionTravelTimeTable, prop)
    sectionTravelTimeFrame
  }

}

object SectionTravelTimeLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sectionTravelTimeLoad = new SectionTravelTimeLoad
    val sectionTravelFrame = sectionTravelTimeLoad.load()
    getSectionTravelMap(sectionTravelFrame).forEach((x, y) => {
      println(s"区间为$x 旅行时间为${y.getSeconds}秒 惩罚系数为${y.getRate} ")
    })
  }

  /**
    * 传入section_travel_time表中的数据DataFrame得到一个可以通过section得到的旅行时间中位数和惩罚系数
    *
    * @param sectionTravelFrame section_travel_time表中的数据DataFrame
    * @return Section和旅行时间TIME以及惩罚费用系数rate
    */
  def getSectionTravelMap(sectionTravelFrame: DataFrame): util.Map[Section, TravelTimeAndRate] = {
    val sectionTravelTimeWithRateMap = new util.HashMap[Section, TravelTimeAndRate]()
    val sectionRows = sectionTravelFrame.rdd.collect()
    sectionRows.map(x => {
      val section = new Section(x.getString(0), x.getString(1))
      val timeAndRate = new TravelTimeAndRate(x.getDecimal(2).intValue(), x.getDouble(3))
      sectionTravelTimeWithRateMap.put(section, timeAndRate)
    })
    sectionTravelTimeWithRateMap
  }
}
