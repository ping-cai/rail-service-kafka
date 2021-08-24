package control

import java.{lang, util}

import calculate.BaseCalculate
import costcompute.{MinGeneralizedCost, TimeIntervalStationFlow, TimeIntervalTraffic, TimeIntervalTransferFlow}
import distribution.{DistributionResult, StationWithType, TransferWithDirection}
import domain.Section
import flowdistribute.OdWithTime
import utils.TimeKey

trait Control extends Serializable {

  def startup(controlInfo: ControlInfo)

}

object Control {
  def createDistributionResult() = {
    val timeSectionFlow = new util.HashMap[TimeKey, util.Map[Section, lang.Double]]()
    val timeIntervalTraffic = new TimeIntervalTraffic(timeSectionFlow)
    val timeStationFlow = new util.HashMap[TimeKey, util.Map[StationWithType, lang.Double]]()
    val timeIntervalStationFlow = new TimeIntervalStationFlow(timeStationFlow)
    val timeTransferFlow = new util.HashMap[TimeKey, util.Map[TransferWithDirection, lang.Double]]()
    val timeIntervalTransferFlow = new TimeIntervalTransferFlow(timeTransferFlow)
    val distributionResult = new DistributionResult(timeIntervalTraffic, timeIntervalStationFlow, timeIntervalTransferFlow)
    distributionResult
  }

  def tryCost(baseCalculate: BaseCalculate, kspNumber: Int, odWithTime: OdWithTime) = {
    val legalPath = baseCalculate.getLegalPathList(kspNumber, odWithTime)
    val staticCost = baseCalculate.getStaticCost(legalPath)
    val minGeneralizedCost = new MinGeneralizedCost().compose(staticCost)
    //    合法的路径集合，静态广义费用，最小广义费用
    (legalPath, staticCost, minGeneralizedCost)
  }
}

