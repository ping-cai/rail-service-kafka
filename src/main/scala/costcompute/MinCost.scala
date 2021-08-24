package costcompute

import java.util

import kspcalculation.Path

trait MinCost extends Serializable {
  def compose(sectionMoveCost: util.Map[Path, Double]): (Path, Double)
}
