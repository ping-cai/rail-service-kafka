package costcompute

import java.util

import kspcalculation.Path

class TravelGeneralizedCost {

  def getTravelGeneralizedCost(sectionMoveCost: util.Map[Path, Double], travelGeneralizedCost: util.Map[Path, Double], stopTimeCost: util.Map[Path, Double]): util.Map[Path, Double] = {
    sectionMoveCost.forEach((path, costs) => {
      sectionMoveCost.put(path, costs + travelGeneralizedCost.get(path) + stopTimeCost.get(path))
    })
    sectionMoveCost
  }

}
