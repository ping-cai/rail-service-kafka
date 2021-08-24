package distribution

import conf.DynamicConf

trait LogitDistribute extends Serializable {
  //logit模型分配
  def logit(passengers: Double, startTime: String): DistributionResult
}

object LogitDistribute {
  //  分配参数，与路权无关，通常在3-3.5之间
  val theta = DynamicConf.theta
}