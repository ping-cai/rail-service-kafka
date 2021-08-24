package calculate

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import dataload.HDFSODLoad
import dataload.base.{OracleSectionLoad, OracleTransferLoad, SectionTravelTimeLoad}
import domain.ODWithSection
import entity.GraphWithSectionAssociation
import flowdistribute._
import flowreduce.TimeSectionMap
import kspcalculation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import result.{LineFlow, ODWithSectionFlow, SectionFlow}

import scala.util.Try

//  构建的路网图基础数据，计算K短路,蕴含区间所有信息，以及区间所有信息
class OdCutTime(private var odOriginPath: String, private var graphWithSectionAssociation: GraphWithSectionAssociation) extends Serializable {
  val checkPointPath = s"hdfs://hacluster/data/checkPoint/${new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())}"
  private var distributionType: String = "static"
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var kspNum: Int = _

  def this(odOriginPath: String, distributionType: String, graphWithSectionAssociation: GraphWithSectionAssociation) {
    this(odOriginPath, graphWithSectionAssociation)
    this.distributionType = distributionType
  }

  def this(odOriginPath: String, distributionType: String,
           graphWithSectionAssociation: GraphWithSectionAssociation,
           kspNum: Int) {
    this(odOriginPath, graphWithSectionAssociation)
    this.distributionType = distributionType
    this.kspNum = kspNum
  }

  /**
    *
    * @return (lineFlow, sectionFlow, odDataRdd) 线路流量数据，区间流量数据，错误OD数据
    */
  def OdCutTimeActive(sparkSession: SparkSession) = {
    val hdfsOdLoad = new HDFSODLoad(odOriginPath)
    val odWithTimeRdd: RDD[OdWithTime] = hdfsOdLoad.getOdRdd(sparkSession)
    import sparkSession.implicits._
    sparkSession.sparkContext.setCheckpointDir(checkPointPath)
    val rddTuple = rddOption(odWithTimeRdd)
    rddTuple.persist(StorageLevel.MEMORY_AND_DISK_SER)
    rddTuple.checkpoint()
    val sectionRdd = rddTuple.filter(x => x.isSuccess).map(x => x.get._1)
    val lineRdd = rddTuple.filter(x => x.isSuccess).map(x => x.get._2)
    val odWithSectionFlowRdd = rddTuple.filter(x => x.isSuccess).map(x => x.get._3)
    val errorRdd = rddTuple.filter(x => x.isFailure).map(x => x.failed.get.getMessage).toDF("error_message")
    val lineFlow = new LineFlow().getFlow(sparkSession, lineRdd)
    val sectionFlow = new SectionFlow().getFlow(sparkSession, sectionRdd)
    val odWithSectionFrame = new ODWithSectionFlow().getFlow(sparkSession, odWithSectionFlowRdd)
    (lineFlow, sectionFlow, odWithSectionFrame, errorRdd)
  }

  def rddOption(odWithTimeRdd: RDD[OdWithTime]) = {
    val associationMap = graphWithSectionAssociation.getAssociationMap
    val sectionFlow = new SectionFlow(graphWithSectionAssociation)
    //    这里是对OD进行分布式K路径计算的地方
    odWithTimeRdd.map(odWithTime => {
      Try(odCutTimeExceptionHandle(associationMap, sectionFlow, odWithTime))
    })
  }

  private def odCutTimeExceptionHandle(associationMap: SectionAssociationMap, sectionFlow: SectionFlow, odWithTime: OdWithTime) = {
    val simpleLogitResult = compute(odWithTime)
    val sectionFlows = sectionFlow.sectionResult(simpleLogitResult)
    val lineFlows = LineFlow.lineResult(associationMap, simpleLogitResult)
    val timeSectionMap = new TimeSectionMap(sectionFlows.getSimpleSectionMap)
    val odWithSection = new ODWithSection(odWithTime, timeSectionMap)
    (sectionFlows, lineFlows, odWithSection)
  }

  /**
    *
    * @param odWithTime 含有时间的OD数据信息
    * @return
    */
  def compute(odWithTime: OdWithTime) = {
    val graph = graphWithSectionAssociation.getGraph
    val associationMap = graphWithSectionAssociation.getAssociationMap
    val paths = new PathComputeService(kspNum).fixedPathOdCompute(graph, odWithTime)
    val pathWithTransferNum: util.Map[Path, Integer] = PathComputeService.removeTheFirstAndLastTransfer(associationMap, paths)
    val kPathResult = new KPathResult(odWithTime, pathWithTransferNum)
    val staticService = new LogitModelComputeService(distributionType)
    LogitModelComputeService.logit(kPathResult, staticService)
  }
}

object OdCutTime {
  //  这里进行测试，测试只有一个OD时的分配结果是否可以得出
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    //    //    od数据地址
    //    val odPath = args(1)
    //    //    分配类型
    //    val distributionType = args(2)
    //    //    线路分配结果路径
    //    val lineResultPath = args(3)
    //    //    区间分配结果路径
    //    val sectionResultPath = args(4)
    val odPath = ""
    val distributionType = "dynamic"
    val lineResultPath = ""
    val sectionResultPath = ""
    val accessRoadInfo = new AccessRoadInfo(odPath = odPath, distributionType = distributionType,
      lineResultPath = lineResultPath, sectionResultPath = sectionResultPath)
    val sectionLoad = new OracleSectionLoad()
    val transferLoad = new OracleTransferLoad()
    val sectionTravelTimeLoad = new SectionTravelTimeLoad()
    val graphWithSectionAssociation = accessRoadInfo.sectionLoad(sectionLoad, transferLoad, sectionTravelTimeLoad)
    val odCutTime = new OdCutTime(odOriginPath = odPath, distributionType = distributionType, graphWithSectionAssociation = graphWithSectionAssociation)
    //    accessRoadInfo.odLoadToCompute(association, odCutTime)
    val odTime = new OdWithTime("77", "121", "2021-01-02 15:11:35", 3.0)
    val simpleLogitResult = odCutTime.compute(odTime)
    val sectionFlow = new SectionFlow(graphWithSectionAssociation)
    val sectionResult = sectionFlow.sectionResult(simpleLogitResult)
    sectionResult.getSimpleSectionMap.forEach((x, y) => {
      y.forEach((section, flow) => {
        println(s"时间段为$x 区间为$section 流量为$flow")
      })

    })
  }
}
