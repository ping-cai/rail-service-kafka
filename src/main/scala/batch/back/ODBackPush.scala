package batch.back

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import calculate.back.OptimizeCalculate
import conf.DynamicConf
import dataload.{BaseDataLoad, HDFSODLoad}
import dataload.back.BackSectionLoad
import model.back.{BackPushFirstResult, BackPushSecondResult}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import utils.TimestampUtil

import scala.collection.mutable.ListBuffer

class ODBackPush(sparkSession: SparkSession) extends Serializable {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private val baseDataLoad: Broadcast[BaseDataLoad] = sparkSession.sparkContext.broadcast(new BaseDataLoad)
  private val odCsvFile: String = DynamicConf.odCsvFile
  private val pathNum: Int = DynamicConf.pathNum
  private val backSectionLoad: Broadcast[BackSectionLoad] = sparkSession.sparkContext.broadcast(new BackSectionLoad)
  private val url: String = DynamicConf.localhostUrl
  private val prop: Properties = new Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }

  import sparkSession.implicits._

  def odBackPush(): Unit = {
    sparkSession.sparkContext.setLogLevel("WARN")
    val optimizeCalculate = new OptimizeCalculate(sparkSession, baseDataLoad, backSectionLoad)
    val hDFSODLoad = new HDFSODLoad(odCsvFile)
    val odWithTimeRdd = hDFSODLoad.getOdRdd(sparkSession)
    val pathSaveService = sparkSession.sparkContext.broadcast(new PathSaveService(baseDataLoad.value, pathNum, sparkSession))
    val pathBuffer = optimizeCalculate.computePathList(odWithTimeRdd, pathSaveService)
    val result: RDD[(ListBuffer[BackPushFirstResult], ListBuffer[BackPushSecondResult])] = pathBuffer.map(x => {
      val odWithTime = x._1
      val pathBuffer = x._2
      optimizeCalculate.getResult(odWithTime, pathBuffer)
    })
    saveResult(result)
  }

  private def saveResult(resultRdd: RDD[(ListBuffer[BackPushFirstResult], ListBuffer[BackPushSecondResult])]): Unit = {
    val backPushFirst = resultRdd.flatMap(x => x._1)
    val backPushSecond = resultRdd.flatMap(x => x._2)
    val firstFrame = backPushFirst.toDF()
    val secondFrame = backPushSecond.toDF()
    sparkSession.udf.register("time_interval", (x: Timestamp) => TimestampUtil.timeAgg(x))
    firstFrame.createOrReplaceTempView("first_result")
    secondFrame.createOrReplaceTempView("second_result")
    val firstResultFrame = sparkSession.sql(
      """
select inId,outId,startTime,sectionSeq,sum(seqFlow) seqFlow,sum(totalFlow) totalFlow
from
(select inId,outId,time_interval(time) startTime,sectionSeq,seqFlow,totalFlow
from first_result) origin
where seqFlow>0
group by inId,outId,startTime,sectionSeq
      """.stripMargin)
    val secondResultFrame = sparkSession.sql(
      """
select inId,outId,startTime,sectionSeq,notPassedSectionSeqPathId,sum(pathFlow) pathFlow
from
(select inId,outId,time_interval(time) startTime,sectionSeq,notPassedSectionSeqPathId,pathFlow
from second_result) origin
where pathFlow>0
group by inId,outId,startTime,sectionSeq,notPassedSectionSeqPathId
      """.stripMargin)
    val dateFormat = new SimpleDateFormat("yyyy_MM_dd_HH")
    val currentDate = dateFormat.format(new Date())
    val firstTable = s"back_first_$currentDate"
    val secondTable = s"back_second_$currentDate"
    firstResultFrame.write.mode("append").jdbc(url, firstTable, prop)
    secondResultFrame.write.mode("append").jdbc(url, secondTable, prop)
  }
}

object ODBackPush {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("ODBackPush").getOrCreate()
    val oDBackPush = new ODBackPush(sparkSession)
    oDBackPush.odBackPush()
  }

}
