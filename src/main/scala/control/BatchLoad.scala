package control

import distribution.DistributionResult
import flowdistribute.OdWithTime
import kspcalculation.Path

/**
  * 实现分批加载逻辑
  */
trait BatchLoad extends Serializable {
  def batchDistribute(odWithTime: OdWithTime,
                      pathDynamicCost: java.util.Map[Path, Double],
                      minPathCost: (Path, Double)): DistributionResult
}
