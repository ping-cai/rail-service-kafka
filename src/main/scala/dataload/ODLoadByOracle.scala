package dataload

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import conf.DynamicConf
import flowdistribute.OdWithTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.TimestampUtil

class ODLoadByOracle(sparkSession: SparkSession) extends Serializable {
  private val url: String = DynamicConf.localhostUrl
  private val prop: Properties = new Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }

  def getOdRdd(startTime: Timestamp): RDD[OdWithTime] = {
    val startDate = startTime.toLocalDateTime
    val year = startDate.getYear - 2000
    val month = startDate.getMonth.getValue
    val week = startDate.getDayOfMonth / 7
    val kalmanOd = s"KALMAN_OD_${year}_${month}_$week"
    val dataFrame = sparkSession.read.jdbc(url, kalmanOd, prop)
    val result = dataFrame.rdd.map(x => {
      val timestamp = x.getTimestamp(0)
      val inId = x.getString(1)
      val outId = x.getString(2)
      val passengers = x.getDecimal(3).doubleValue()
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val startTime = dateFormat.format(timestamp)
      new OdWithTime(inId, outId, startTime, passengers)
    })
    result
  }
}

object ODLoadByOracle {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val oDLoadByOracle = new ODLoadByOracle(sparkSession)
    val start = "2021-07-01 00:00:00"
    val end = "2021-07-28 00:00:00"
    val startTime = Timestamp.valueOf(start)
    val endTime = Timestamp.valueOf(end)
    val weekNum = (endTime.toLocalDateTime.getDayOfMonth - startTime.toLocalDateTime.getDayOfMonth) / 7
    val currentTime = TimestampUtil.addWeek(startTime, 1)
    val odRdd = oDLoadByOracle.getOdRdd(currentTime)
    odRdd.foreach(println(_))
  }
}
