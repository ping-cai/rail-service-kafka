package save

import java.{lang, util}

import costcompute.TimeIntervalTraffic
import domain.{Section, SectionInfo}
import domain.dto.SectionDataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.TimeKey

import scala.collection.JavaConverters._

class SectionSave(sectionTable: String, sectionInfoMap: util.Map[Section, SectionInfo]) extends Save[TimeIntervalTraffic]
  with SaveRdd[TimeIntervalTraffic] {
  override def save(resultData: TimeIntervalTraffic, sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val sectionResult: List[(TimeKey, util.Map[Section, lang.Double])] = resultData.getTimeSectionTraffic.asScala.toList
    val sectionResultRdd = sc.makeRDD(sectionResult)
    val sectionFrameRdd = sectionResultRdd.flatMap(timeKeyWithSection => {
      val timeKey = timeKeyWithSection._1
      val sectionFlowMap = timeKeyWithSection._2
      sectionFlowMap.asScala.map(sectionFlow => {
        val section = sectionFlow._1
        val flow = sectionFlow._2
        val sectionId = sectionInfoMap.get(section).getSectionId
        SectionDataFrame(timeKey.getStartTime, timeKey.getEndTime, sectionId, section.getInId, section.getOutId, flow)
      })
    })
    val sectionFrame = sectionFrameRdd.toDF()
    sectionFrame.createOrReplaceTempView("section_result")
    val sumFlowSql = "select START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID,round(PASGR_FLOW_QTTY,18) PASGR_FLOW_QTTY " +
      "from " +
      "(select START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID,SUM(PASGR_FLOW_QTTY) PASGR_FLOW_QTTY " +
      "FROM section_result group by START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID)"
    sparkSession.sql(sumFlowSql).write.mode("append").jdbc(Save.url, sectionTable, Save.prop)
  }

  override def saveByRdd(resultRdd: RDD[TimeIntervalTraffic], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val sectionFrameRdd = resultRdd.flatMap(timeIntervalTraffic => {
      timeIntervalTraffic.getTimeSectionTraffic.asScala.flatMap(timeKeyWithSection => {
        val timeKey = timeKeyWithSection._1
        val sectionFlowMap = timeKeyWithSection._2
        sectionFlowMap.asScala.filter(sectionFlow => {
          !sectionFlow._2.isNaN
        }).map(sectionFlow => {
          val section = sectionFlow._1
          val flow = sectionFlow._2
          val sectionId = sectionInfoMap.get(section).getSectionId
          SectionDataFrame(timeKey.getStartTime, timeKey.getEndTime, sectionId, section.getInId, section.getOutId, flow)
        }
        )
      })
    })
    val sectionFrame = sectionFrameRdd.toDF()
    sectionFrame.createOrReplaceTempView("section_result")
    val sumFlowSql = "select START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID,round(PASGR_FLOW_QTTY,18) PASGR_FLOW_QTTY " +
      "from " +
      "(select START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID,SUM(PASGR_FLOW_QTTY) PASGR_FLOW_QTTY " +
      "FROM section_result group by START_TIME,END_TIME,SECTION_ID,START_STATION_ID,END_STATION_ID)"
    sparkSession.sql(sumFlowSql).write.mode("append").jdbc(Save.url, sectionTable, Save.prop)
  }
}
