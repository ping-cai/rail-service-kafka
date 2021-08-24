package calculate

import costcompute.TimeIntervalTraffic
import dataload.BaseDataLoad
import distribution.DynamicResult
import flowdistribute.OdWithTime

trait Calculate extends Serializable {
  def calculate(baseDataLoad: BaseDataLoad, timeIntervalTraffic: TimeIntervalTraffic, odWithTime: OdWithTime, interval: Int): DynamicResult
}
