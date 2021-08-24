package save

import costcompute.TimeIntervalTransferFlow
import distribution.TransferWithDirection
import domain.dto.TransferDataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.TimestampUtil

import scala.collection.JavaConverters._

class TransferSave(transferTable: String, transferLineMap: java.util.Map[String, String]) extends Save[TimeIntervalTransferFlow]
  with SaveRdd[TimeIntervalTransferFlow] {
  override def save(resultData: TimeIntervalTransferFlow, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val transferResult = resultData.getTimeIntervalTransferFlow.asScala.toList
    val transferResultRdd = sc.makeRDD(transferResult)
    val transferFrameRdd = transferResultRdd.flatMap(timeKeyWithTransfer => {
      val timeKey = timeKeyWithTransfer._1
      val startTime = timeKey.getStartTime
      val endTime = timeKey.getEndTime
      timeKeyWithTransfer._2.asScala.map(transferFlow => {
        val transfer: TransferWithDirection = transferFlow._1
        val direction = transfer.getDirection
        val section = transfer.getSection
        val inId = section.getInId
        val outId = section.getOutId
        val inLine = transferLineMap.get(inId)
        val outLine = transferLineMap.get(outId)
        val flow = transferFlow._2
        direction match {
          case "11" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, flow, 0, 0, 0)
          case "12" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, flow, 0, 0)
          case "21" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, flow, 0)
          case "22" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, 0, flow)
        }
      })
    })
    val transferFrame = transferFrameRdd.toDF()
    transferFrame.createOrReplaceTempView("transfer_result")
    val sumFlowSql = "select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "round(TRAF_SIN_SOUT,18) TRAF_SIN_SOUT,round(TRAF_SIN_XOUT,18) TRAF_SIN_XOUT,round(TRAF_XIN_SOUT,18) TRAF_XIN_SOUT,round(TRAF_XIN_XOUT,18) TRAF_XIN_XOUT " +
      "from " +
      "(select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "SUM(TRAF_SIN_SOUT) TRAF_SIN_SOUT,SUM(TRAF_SIN_XOUT) TRAF_SIN_XOUT,SUM(TRAF_XIN_SOUT) TRAF_XIN_SOUT,SUM(TRAF_XIN_XOUT) TRAF_XIN_XOUT " +
      "from transfer_result group by START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID)"
    val resultFrame = sparkSession.sql(sumFlowSql)
    resultFrame.write.mode("append").jdbc(Save.url, transferTable, Save.prop)
  }

  override def saveByRdd(resultRdd: RDD[TimeIntervalTransferFlow], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val transferFrameRdd = resultRdd.flatMap(timeIntervalTransferFlow => {
      timeIntervalTransferFlow.getTimeIntervalTransferFlow.asScala.flatMap(
        timeKeyWithTransfer => {
          val timeKey = timeKeyWithTransfer._1
          val startTime = timeKey.getStartTime
          val endTime = timeKey.getEndTime
          timeKeyWithTransfer._2.asScala.filter(transferFlow => {
            !transferFlow._2.isNaN
          }).map(transferFlow => {
            val transfer: TransferWithDirection = transferFlow._1
            val direction = transfer.getDirection
            val section = transfer.getSection
            val inId = section.getInId
            val outId = section.getOutId
            val inLine = transferLineMap.get(inId)
            val outLine = transferLineMap.get(outId)
            val flow = transferFlow._2
            direction match {
              case "11" =>
                TransferDataFrame(startTime, endTime, outId, inLine, outLine, flow, 0, 0, 0)
              case "12" =>
                TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, flow, 0, 0)
              case "21" =>
                TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, flow, 0)
              case "22" =>
                TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, 0, flow)
            }
          })
        }
      )
    })
    val transferFrame = transferFrameRdd.toDF()
    transferFrame.createOrReplaceTempView("transfer_result")
    val sumFlowSql = "select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "round(TRAF_SIN_SOUT,18) TRAF_SIN_SOUT,round(TRAF_SIN_XOUT,18) TRAF_SIN_XOUT,round(TRAF_XIN_SOUT,18) TRAF_XIN_SOUT,round(TRAF_XIN_XOUT,18) TRAF_XIN_XOUT " +
      "from " +
      "(select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "SUM(TRAF_SIN_SOUT) TRAF_SIN_SOUT,SUM(TRAF_SIN_XOUT) TRAF_SIN_XOUT,SUM(TRAF_XIN_SOUT) TRAF_XIN_SOUT,SUM(TRAF_XIN_XOUT) TRAF_XIN_XOUT " +
      "from transfer_result group by START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID)"
    val resultFrame = sparkSession.sql(sumFlowSql)
    resultFrame.write.mode("append").jdbc(Save.url, transferTable, Save.prop)
  }

  def save2Half(resultData: TimeIntervalTransferFlow, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val transferResult = resultData.getTimeIntervalTransferFlow.asScala.toList
    val transferResultRdd = sc.makeRDD(transferResult)
    val transferFrameRdd = transferResultRdd.flatMap(timeKeyWithTransfer => {
      val timeKey = timeKeyWithTransfer._1
      val startTime = TimestampUtil.quarter2Half(timeKey.getStartTime)
      val endTime = TimestampUtil.quarter2Half(timeKey.getEndTime)
      timeKeyWithTransfer._2.asScala.map(transferFlow => {
        val transfer: TransferWithDirection = transferFlow._1
        val direction = transfer.getDirection
        val section = transfer.getSection
        val inId = section.getInId
        val outId = section.getOutId
        val inLine = transferLineMap.get(inId)
        val outLine = transferLineMap.get(outId)
        val flow = transferFlow._2
        direction match {
          case "11" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, flow, 0, 0, 0)
          case "12" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, flow, 0, 0)
          case "21" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, flow, 0)
          case "22" =>
            TransferDataFrame(startTime, endTime, outId, inLine, outLine, 0, 0, 0, flow)
        }
      })
    })
    val transferFrame = transferFrameRdd.toDF()
    transferFrame.createOrReplaceTempView("transfer_result")
    val sumFlowSql = "select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "round(TRAF_SIN_SOUT,18) TRAF_SIN_SOUT,round(TRAF_SIN_XOUT,18) TRAF_SIN_XOUT,round(TRAF_XIN_SOUT,18) TRAF_XIN_SOUT,round(TRAF_XIN_XOUT,18) TRAF_XIN_XOUT " +
      "from " +
      "(select START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID," +
      "SUM(TRAF_SIN_SOUT) TRAF_SIN_SOUT,SUM(TRAF_SIN_XOUT) TRAF_SIN_XOUT,SUM(TRAF_XIN_SOUT) TRAF_XIN_SOUT,SUM(TRAF_XIN_XOUT) TRAF_XIN_XOUT " +
      "from transfer_result group by START_TIME,END_TIME,XFER_STATION_ID,TRAF_IN_LINE_ID,TRAF_OUT_LINE_ID)"
    val resultFrame = sparkSession.sql(sumFlowSql)
    resultFrame.write.mode("append").jdbc(Save.url, transferTable, Save.prop)
  }
}
