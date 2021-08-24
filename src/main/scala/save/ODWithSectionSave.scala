package save

import java.util

import distribution.ODWithSectionResult
import domain.{Section, SectionInfo}
import domain.dto.ODShareWithPassengers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class ODWithSectionSave(savePath: String, sectionInfoMap: util.Map[Section, SectionInfo]) extends Save[java.util.List[ODWithSectionResult]] with SaveRdd[ODWithSectionResult] {

  override def save(resultData: util.List[ODWithSectionResult], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val odWithDistributionRdd = sparkSession.sparkContext.makeRDD(resultData.asScala.toList)
    val odShareRdd = odWithDistributionRdd.flatMap(
      odWithDistribution => {
        val odWithTime = odWithDistribution.getOdWithTime
        val origin = odWithTime.getInId
        val destination = odWithTime.getOutId
        val inTime = odWithTime.getInTime
        val outTime = odWithTime.getOutTime
        val passengers = odWithTime.getPassengers
        odWithDistribution.getTempSectionResult.getTimeSectionTraffic.asScala.flatMap(
          timeKeyWithSectionFlowMap => {
            val timeKey = timeKeyWithSectionFlowMap._1
            val timeKeyStart = timeKey.getStartTime
            val timeKeyEnd = timeKey.getEndTime
            timeKeyWithSectionFlowMap._2.asScala.map(
              sectionFlow => {
                val section = sectionFlow._1
                val startId = section.getInId
                val endId = section.getOutId
                val flow = sectionFlow._2
                val sectionId = sectionInfoMap.get(section).getSectionId
                ODShareWithPassengers(timeKeyStart, timeKeyEnd, sectionId, startId, endId, origin, destination, inTime, outTime, flow, passengers)
              }
            )
          }
        )
      }
    )
    val odShareFrame = odShareRdd.filter(odShare => odShare.odShareFlow > 0.0001).toDF()
    odShareFrame.write.mode("append").csv(savePath)
  }

  override def saveByRdd(resultRdd: RDD[ODWithSectionResult], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val odShareRdd = resultRdd.flatMap(
      odWithDistribution => {
        val odWithTime = odWithDistribution.getOdWithTime
        val origin = odWithTime.getInId
        val destination = odWithTime.getOutId
        val inTime = odWithTime.getInTime
        val outTime = odWithTime.getOutTime
        val passengers = odWithTime.getPassengers
        odWithDistribution.getTempSectionResult.getTimeSectionTraffic.asScala.flatMap(
          timeKeyWithSectionFlowMap => {
            val timeKey = timeKeyWithSectionFlowMap._1
            val timeKeyStart = timeKey.getStartTime
            val timeKeyEnd = timeKey.getEndTime
            timeKeyWithSectionFlowMap._2.asScala.map(
              sectionFlow => {
                val section = sectionFlow._1
                val startId = section.getInId
                val endId = section.getOutId
                val flow = sectionFlow._2
                val sectionId = sectionInfoMap.get(section).getSectionId
                ODShareWithPassengers(timeKeyStart, timeKeyEnd, sectionId, startId, endId, origin, destination, inTime, outTime, flow, passengers)
              }
            )
          }
        )
      }
    )
    val odShareFrame = odShareRdd.filter(odShare => odShare.odShareFlow > 0.0001).toDF()
    odShareFrame.write.mode("append").csv(savePath)
  }
}
