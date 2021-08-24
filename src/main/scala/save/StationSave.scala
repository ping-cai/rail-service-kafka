package save

import costcompute.TimeIntervalStationFlow
import domain.dto.StationDataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

class StationSave(stationTable: String) extends Save[TimeIntervalStationFlow] with SaveRdd[TimeIntervalStationFlow] {
  override def save(resultData: TimeIntervalStationFlow, sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._
    val stationResult = resultData.getTimeStationTraffic.asScala.toList
    val stationResultRdd = sc.makeRDD(stationResult)
    val stationFrameRdd = stationResultRdd.flatMap(timeKeyWithStation => {
      val timeKey = timeKeyWithStation._1
      val startTime = timeKey.getStartTime
      val endTime = timeKey.getEndTime
      timeKeyWithStation._2.asScala.map(stationFlow => {
        val stationWithType = stationFlow._1
        val flow = stationFlow._2
        if ("in".equals(stationWithType.getType)) {
          StationDataFrame(stationWithType.getStationId, startTime, endTime, flow, 0, flow)
        } else {
          StationDataFrame(stationWithType.getStationId, startTime, endTime, 0, flow, flow)
        }
      })
    })
    val stationFrame = stationFrameRdd.toDF()
    stationFrame.createOrReplaceTempView("station_result")
    val sumFlowSql = "select STATION_ID,START_TIME,END_TIME,round(ENTRY_QUATITY,18) ENTRY_QUATITY,round(EXIT_QUATITY,18) EXIT_QUATITY,round(ENTRY_EXIT_QUATITY,18) ENTRY_EXIT_QUATITY " +
      "from " +
      "(select STATION_ID,START_TIME,END_TIME,SUM(ENTRY_QUATITY) ENTRY_QUATITY,SUM(EXIT_QUATITY) EXIT_QUATITY,SUM(ENTRY_EXIT_QUATITY) ENTRY_EXIT_QUATITY " +
      "FROM station_result group by STATION_ID,START_TIME,END_TIME)"
    sparkSession.sql(sumFlowSql).write.mode("append").jdbc(Save.url, stationTable, Save.prop)
  }

  override def saveByRdd(resultRdd: RDD[TimeIntervalStationFlow], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val stationFrameRdd = resultRdd.flatMap(
      timeIntervalStationFlow => {
        timeIntervalStationFlow.getTimeStationTraffic.asScala.flatMap(
          timeKeyWithStation => {
            val timeKey = timeKeyWithStation._1
            val startTime = timeKey.getStartTime
            val endTime = timeKey.getEndTime
            timeKeyWithStation._2.asScala.filter(stationFlow => {
              !stationFlow._2.isNaN
            }).map(stationFlow => {
              val stationWithType = stationFlow._1
              val flow = stationFlow._2
              if ("in".equals(stationWithType.getType)) {
                StationDataFrame(stationWithType.getStationId, startTime, endTime, flow, 0, flow)
              } else {
                StationDataFrame(stationWithType.getStationId, startTime, endTime, 0, flow, flow)
              }
            })
          }
        )
      }
    )
    val stationFrame = stationFrameRdd.toDF()
    stationFrame.createOrReplaceTempView("station_result")
    val sumFlowSql = "select STATION_ID,START_TIME,END_TIME,round(ENTRY_QUATITY,18) ENTRY_QUATITY,round(EXIT_QUATITY,18) EXIT_QUATITY,round(ENTRY_EXIT_QUATITY,18) ENTRY_EXIT_QUATITY " +
      "from " +
      "(select STATION_ID,START_TIME,END_TIME,SUM(ENTRY_QUATITY) ENTRY_QUATITY,SUM(EXIT_QUATITY) EXIT_QUATITY,SUM(ENTRY_EXIT_QUATITY) ENTRY_EXIT_QUATITY " +
      "FROM station_result group by STATION_ID,START_TIME,END_TIME)"
    sparkSession.sql(sumFlowSql).write.mode("append").jdbc(Save.url, stationTable, Save.prop)
  }
}
