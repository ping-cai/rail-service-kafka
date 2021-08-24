package utils

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.util.Calendar

object TimestampUtil extends Serializable {
  val Minutes = 60000.0
  val defaultGranularity = 15

  def timeAgg(inputTime: Timestamp, aggGranularity: Int): Timestamp = {
    val currentTime = inputTime.getTime
    val aggTime = (currentTime / Minutes / aggGranularity).toLong
    new Timestamp(aggTime * Minutes.toInt * aggGranularity)
  }

  def timeAgg(inputTime: Timestamp): Timestamp = {
    val currentTime = inputTime.getTime
    val aggTime = (currentTime / Minutes / defaultGranularity).toLong
    new Timestamp(aggTime * Minutes.toInt * defaultGranularity)
  }

  def addWeek(startTime: Timestamp, week: Int): Timestamp = {
    val time = startTime.getTime
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.add(Calendar.WEEK_OF_MONTH, week)
    Timestamp.from(calendar.toInstant)
  }

  def quarter2Half(time: String): String = {
    val half = 1000 * 60 * 30
    val timestamp = Timestamp.valueOf(time)
    val halfLong = (timestamp.getTime / half.toDouble).toLong * half
    timestamp.setTime(halfLong)
    // 24小时
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    timestamp.toLocalDateTime.format(formatter)
  }

  def main(args: Array[String]): Unit = {
    println(quarter2Half("2021-08-21 20:41:01"))
  }
}