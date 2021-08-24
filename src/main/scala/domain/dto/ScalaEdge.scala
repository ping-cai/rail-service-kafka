package domain.dto

import kspcalculation.Path

import scala.collection.JavaConverters._

case class ScalaEdge(fromNode: String, toNode: String, weigh: Double) {
}

object ScalaEdge {
  def createEdgeList(path: Path): List[ScalaEdge] = {
    val edges = path.getEdges
    val scalaEdges = edges.asScala.flatMap(edge => {
      List(ScalaEdge(edge.getFromNode, edge.getToNode, edge.getWeight))
    })
    scalaEdges.toList
  }
}
