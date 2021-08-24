package calculate

import entity.GraphWithSectionAssociation
import exception.PathNotHaveException
import flowdistribute.{LogitModelComputeService, OdWithTime}
import flowreduce.{LineResult, SectionComputeService}
import kspcalculation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import utils.TimeKey

import scala.collection.JavaConverters._
import scala.collection.mutable

class OdWithoutCut extends Serializable {
  private var log: Logger = LoggerFactory.getLogger(this.getClass)
  private var odOriginPath: String = _
  private var distributionType: String = "dynamic"

  def this(odOriginPath: String) {
    this()
    this.odOriginPath = odOriginPath
  }

  def this(odOriginPath: String, distributionType: String) {
    this()
    this.odOriginPath = odOriginPath
    this.distributionType = distributionType
  }

  def getLineMapByTime(sparkSession: SparkSession, graph: Graph, associationMap: SectionAssociationMap): RDD[mutable.Map[TimeKey, LineResult]] = {
    val schema: StructType = StructType(
      Array(
        StructField("id", StringType, nullable = true),
        StructField("kind", IntegerType, nullable = true),
        StructField("in_number", IntegerType, nullable = true),
        StructField("in_time", StringType, nullable = true),
        StructField("out_number", IntegerType, nullable = true),
        StructField("out_time", StringType, nullable = true)
      )
    )
    val dataFrame = sparkSession.read
      .schema(schema)
      .csv(odOriginPath)
    dataFrame.createOrReplaceTempView("od")
    val idTransferSql = "select inTable.CZ_ID,in_time,outTable.CZ_ID,out_time " +
      "from od " +
      "join id_transfer inTable " +
      "on od.in_number=inTable.STATIONID " +
      "join id_transfer outTable " +
      "on od.out_number=outTable.STATIONID"
    val transFrame = sparkSession.sql(idTransferSql)
    val lineMap = transFrame.rdd.map(
      x => {
        val inId = x.getDecimal(1).toString
        val inTime = x.getString(2)
        val outId = x.getDecimal(3).toString
        val outTime = x.getString(4)
        val odWithTime = new OdWithTime(inId, outId, inTime, outTime)
        var keyToResult: mutable.Map[TimeKey, LineResult] = mutable.Map[TimeKey, LineResult]()
        try {
          keyToResult = OdWithoutCut.odWithTimeToComputeKsp(graph, associationMap, odWithTime, distributionType)
        } catch {
          case pathE: PathNotHaveException =>
            log.warn(pathE.getMessage)
        }
        keyToResult
      }
    )
    lineMap
  }
}

object OdWithoutCut {
  def odWithTimeToComputeKsp(graph: Graph, associationMap: SectionAssociationMap, odWithTime: OdWithTime, distributionType: String) = {
    val graphWithSectionAssociation = new GraphWithSectionAssociation(graph, associationMap)
    var paths: java.util.List[Path] = null
    try {
      paths = new PathComputeService(10).fixedPathOdCompute(graph, odWithTime)
    } catch {
      case pathE: PathNotHaveException => {
        throw pathE
      }
    }
    val pathWithTransferNum = PathComputeService.removeTheFirstAndLastTransfer(associationMap, paths)
    val kPathResult = new KPathResult(odWithTime, pathWithTransferNum)
    val staticService = new LogitModelComputeService(distributionType)
    val logitComputeResult = LogitModelComputeService.logit(kPathResult, staticService)
    val timeMapContainSectionMap = SectionComputeService.getSectionBylogitResult(logitComputeResult, graphWithSectionAssociation)
    val resultMap = timeMapContainSectionMap.getLineResultMap.asScala
    resultMap
  }
}
