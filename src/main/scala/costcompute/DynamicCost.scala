package costcompute

import java.util

import kspcalculation.Path

/**
  * 这里计算出列车拥挤的广义费用后，和静态广义费用叠加即可得到动态广义费用
  *
  * @param trainCrowdCost 计算列车拥挤度所需要的参数
  * @param staticPathCost 原来静态计算已经得到广义费用
  */
class DynamicCost(trainCrowdCost: TrainCrowdCost, staticPathCost: util.Map[Path, Double]) extends Cost {

  override def costCompute(legalPath: util.List[Path], sectionTravelGraph: SectionTravelGraph): util.Map[Path, Double] = {
    val trainCrowdPathCost: util.Map[Path, Double] = trainCrowdCost.compose(legalPath)
    trainCrowdPathCost.forEach((path, cost) => {
      val staticCost = staticPathCost.get(path)
      trainCrowdPathCost.put(path, cost + staticCost)
    })
    trainCrowdPathCost
  }
}
