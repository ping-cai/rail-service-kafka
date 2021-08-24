package save

import java.util

import domain.dto.LineDataFrame
import domain.{LinePassengers, Section, SectionInfo}
import kspcalculation.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class LineSave(lineTable: String) extends Save[java.util.List[LinePassengers]] with SaveRdd[LinePassengers] {

  override def save(resultData: util.List[LinePassengers], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val lineResultRdd = sparkSession.sparkContext.makeRDD(resultData.asScala.toList)
    val lineFrame = lineResultRdd.map(line => {
      val lineName = line.getLineName
      val flow = line.getFlow.floatValue()
      LineDataFrame(lineName, flow)
    }).toDF()
    lineFrame.createOrReplaceTempView("line_result")
    val sumFlowSql = "select LINE_NAME,round(PASSENGERS,18) PASSENGERS from " +
      "(select lineName LINE_NAME,SUM(flow) PASSENGERS from line_result group by lineName)"
    val resultFrame = sparkSession.sql(sumFlowSql)
    resultFrame.write.mode("append").jdbc(Save.url, lineTable, Save.prop)
  }

  override def saveByRdd(resultRdd: RDD[LinePassengers], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val lineFrame = resultRdd.filter(line => {
      !line.getFlow.isNaN
    }).map(
      line => {
        val lineName = line.getLineName
        val flow = line.getFlow.floatValue()
        LineDataFrame(lineName, flow)
      }
    ).toDF()
    lineFrame.createOrReplaceTempView("line_result")
    val sumFlowSql = "select LINE_NAME,round(PASSENGERS,18) PASSENGERS from " +
      "(select lineName LINE_NAME,SUM(flow) PASSENGERS from line_result group by lineName)"
    val resultFrame = sparkSession.sql(sumFlowSql)
    resultFrame.write.mode("append").jdbc(Save.url, lineTable, Save.prop)
  }
}

object LineSave {
  def lineFlow(logitResult: util.Map[Path, Double], sectionWithLineMap: util.Map[Section, SectionInfo]): util.List[LinePassengers] = {
    val lineFlowList = new util.ArrayList[LinePassengers](200)
    logitResult.forEach((path, flow) => {
      val edges = path.getEdges
      val lineSet = new util.HashSet[LinePassengers](20)
      edges.forEach(edge => {
        val section = Section.createSectionByEdge(edge)
        val sectionInfo = sectionWithLineMap.get(section)
        if (null != sectionInfo) {
          val lineName = sectionInfo.getLine
          val linePassengers = new LinePassengers(lineName, flow)
          lineSet.add(linePassengers)
        }
      })
      lineFlowList.addAll(lineSet)
    })
    lineFlowList
  }
}
