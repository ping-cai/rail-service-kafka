package result

import domain.ODWithSection
import domain.dto.ODShare
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
  * 计算区间客流的OD分担量
  * 一个OD客流会对应分配很多路径
  * 每条路径分配到的客流数不一致，根据Logit模型分配好的
  * 每条路径上的区间人数是一致的
  * 每条路径上的每一个区间都对应同一个OD
  * 对此进行叠加计算
  */
class ODWithSectionFlow extends Flow[ODWithSection] {
  override def getFlow(sparkSession: SparkSession, rddTuple: RDD[ODWithSection]): DataFrame = {
    val odShareRdd = rddTuple.flatMap(odWithSection => {
      //      有起止时间的OD
      val odWithTime = odWithSection.getOdWithTime
      val origin = odWithTime.getInId
      val destination = odWithTime.getOutId
      val inTime = odWithTime.getInTime
      val outTime = odWithTime.getOutTime
      //      OD分配后的区间流量集合
      val timeSectionMap = odWithSection.getTimeSectionMap
      //      详细属性信息
      val timeKeySectionMap = timeSectionMap.getTimeKeyMapMap
      val odShareSet = timeKeySectionMap.asScala.flatMap(timeKeySection => {
        val timeKey = timeKeySection._1
        val timeKeyStart = timeKey.getStartTime
        val timeKeyEnd = timeKey.getEndTime
        val simpleSectionMap = timeKeySection._2.asScala
        val odShareIterable = simpleSectionMap.map(simple => {
          val simpleSection = simple._1
          val sectionId = simpleSection.getSectionId
          val inId = simpleSection.getInId
          val outId = simpleSection.getOutId
          val flow = simple._2
          ODShare(timeKeyStart, timeKeyEnd, sectionId, inId, outId,
            origin, destination, inTime, outTime, flow)
        })
        odShareIterable
      }).filter(odShare => odShare.odShareFlow > 0.001)
      odShareSet
    })
    import sparkSession.implicits._
    val odShareFrame = odShareRdd.toDF()
    odShareFrame.createTempView("od_section_flow")
    val sumFlow = "select timeKeyStart,timeKeyEnd,sectionId,startId,endId,origin,destination,inTime,outTime,sum(odShareFlow) flow " +
      "from od_section_flow group by timeKeyStart,timeKeyEnd,sectionId,startId,endId,origin,destination,inTime,outTime"
    val finalOdShareFrame = sparkSession.sql(sumFlow)
    finalOdShareFrame
  }

}

object ODWithSectionFlow {
  def main(args: Array[String]): Unit = {

  }
}