package dataload

import costcompute.SectionTravelGraph
import dataload.base.OracleSectionTravelTimeLoad
import kspcalculation.{Edge, Graph}
import org.apache.spark.sql.SparkSession

class LoadTheKPathAndSectionGraph(private var baseDataLoad: BaseDataLoad) {
  private var graph: Graph = _
  private var sectionTravelGraph: SectionTravelGraph = _
  init()

  def getGraph: Graph = {
    this.graph
  }

  def getSectionTravelGraph: SectionTravelGraph = {
    this.sectionTravelGraph
  }

  private def init(): Unit = {
    val sectionList = baseDataLoad.getSectionList
    val transferList = baseDataLoad.getTransferList
    val edges = Edge.getEdgeBySections(sectionList)
    edges.addAll(Edge.getEdgeBySections(transferList))
    val graph = Graph.createGraph(edges)
    this.graph = graph
    val travelTimeLoad = new OracleSectionTravelTimeLoad()
    val travelTimeFrame = travelTimeLoad.load()
    val sectionTravelTimeList = travelTimeLoad.getSectionTravelTimeList(travelTimeFrame)
    val sectionTravelGraph: SectionTravelGraph = SectionTravelGraph.createTravelGraph(sectionTravelTimeList)
    this.sectionTravelGraph = sectionTravelGraph
  }
}

object LoadTheKPathAndSectionGraph {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    testInit()
  }

  def testInit(): Unit = {
    val baseDataLoad = new BaseDataLoad
    val loadTheKPathAndSectionGraph = new LoadTheKPathAndSectionGraph(baseDataLoad)
    loadTheKPathAndSectionGraph.getGraph.getEdgeList.forEach(x => println(x))
    loadTheKPathAndSectionGraph.getSectionTravelGraph.getSectionListMap.forEach((x, y) => println(x, y))
  }
}
