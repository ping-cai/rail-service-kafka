package costcompute

import kspcalculation.Path

trait Cost extends Serializable{
  def costCompute(legalPath: java.util.List[Path],sectionTravelGraph: SectionTravelGraph): java.util.Map[Path, Double]
}
object Cost{

}