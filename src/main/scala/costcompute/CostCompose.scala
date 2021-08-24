package costcompute

import kspcalculation.Path

trait CostCompose extends Serializable {
  /**
    *
    * @param kspResult K短路结果
    * @return 特定的费用
    */
  def compose(kspResult: java.util.List[Path]): java.util.Map[Path, Double]
}

