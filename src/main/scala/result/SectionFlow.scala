package result

import domain.dto.SectionDataFrame
import entity.GraphWithSectionAssociation
import flowdistribute.SimpleLogitResult
import flowreduce.{SectionComputeService, SectionResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class SectionFlow extends Flow[SectionResult] {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var graphWithSectionAssociation: GraphWithSectionAssociation = _

  def this(graphWithSectionAssociation: GraphWithSectionAssociation) {
    this()
    this.graphWithSectionAssociation = graphWithSectionAssociation
  }

  override def getFlow(sparkSession: SparkSession, rddTuple: RDD[SectionResult]): sql.DataFrame = {
    sectionRddOpt(sparkSession, rddTuple)
  }

  private def sectionRddOpt(sparkSession: SparkSession, sectionResult: RDD[SectionResult]) = {
    import sparkSession.implicits._
    val result = sectionResult.flatMap(simpleSection => {
      val simpleSectionMap = simpleSection.getSimpleSectionMap
      val iterableSectionDataFrame = simpleSectionMap.asScala.flatMap(sectionWithTime => {
        val timeKey = sectionWithTime._1
        val sectionDataFrames = sectionWithTime._2.asScala.map(section => {
          val sectionInfo = section._1
          SectionDataFrame(timeKey.getStartTime, timeKey.getEndTime,
            sectionInfo.getSectionId, sectionInfo.getInId, sectionInfo.getOutId, section._2.floatValue())
        })
        sectionDataFrames
      })
      iterableSectionDataFrame
    }).toDF("startTime", "endTime", "sectionId", "inId", "outId", "flow")
    result.createTempView("section_result")
    val sumSql = "select startTime,endTime,sectionId,inId,outId,sum(flow) passengers " +
      "from section_result " +
      "group by startTime,endTime,sectionId,inId,outId"
    val finalResult = sparkSession.sql(sumSql)
    finalResult
  }

  def sectionResult(logitComputeResult: SimpleLogitResult) = {
    val sectionResult = SectionComputeService.getSectionBylogitResult(logitComputeResult, graphWithSectionAssociation)
    sectionResult
  }
}

object SectionFlow {

}
