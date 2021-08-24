package utils

import costcompute.TravelTime

import scala.util.Try

class TravelTimeHandle extends Serializable {

}

object TravelTimeHandle {
  /**
    * 这里是计算某个时段内车辆的到达情况，只需要这个时段包含开始时间即可
    * 格式是15.24.15 15.24.50,转化为2021-02-24 15:15:00 2021-02-24 15:30:00
    *
    * @param travelTime   15.24.15 15.24.50 类似这样的时间
    * @param startDate    数据的时间前缀，比如2021-02-24
    * @param timeInterval 时间粒度 15，30，60
    * @return 类似2021-02-24 15:15:00 2021-02-24 15:30:00这样的时间
    */
  def getTimeKey(travelTime: TravelTime, timeInterval: Int): TimeKey = {
    val startDate = "2021-06-13"
    val arrivalTime = travelTime.getArrivalTime
    val triedGetTime = Try(arrivalTime.split("\\."))
    if (triedGetTime.isSuccess) {
      val timeArray = triedGetTime.get
      var hours = timeArray(0)
      val minutesString = timeArray(1)
      var minutes = ((minutesString.toInt / timeInterval) * timeInterval).toString
      if (hours.toInt < 10) {
        hours = s"0$hours"
      }
      if (minutes.toInt < 10) {
        minutes = s"0$minutes"
      }
      val startTime = s"$startDate $hours:$minutes:00"
      val endTime = DateExtendUtil.timeAddition(startTime, 0, timeInterval)
      val start = startTime.split(" ")(1)
      val end = endTime.split(" ")(1)
      new TimeKey(start, end)
    } else {
      val startTime = s"$startDate 00:00:00"
      new TimeKey(startTime, startTime)
    }
  }
}
