package model.back

case class PathModel(edgeModel: List[EdgeModel], totalCost: Double) {
  override def toString: String = {
    val edgeString = new StringBuffer()
    edgeModel foreach (edge => {
      edgeString append s"${edge.fromNode}->"
    })
    edgeString append edgeModel.last.toNode
    edgeString toString
  }
}
