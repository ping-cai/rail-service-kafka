package result

import java.{lang, util}

import domain.dto.LineDataFrame
import flowdistribute.{LineWithFlow, SimpleLogitResult}
import flowreduce.SectionComputeService
import kspcalculation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

class LineFlow extends Flow[mutable.Buffer[LineWithFlow]] {

  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def getFlow(sparkSession: SparkSession, rddTuple: RDD[mutable.Buffer[LineWithFlow]]): sql.DataFrame = {
    val lineRdd: RDD[mutable.Map[String, Double]] = lineRddOpt(rddTuple)
    val lineFrameList = lineRdd.flatMap(x => {
      x.map(y => {
        List(LineDataFrame(y._1, y._2.floatValue()))
      })
    }).flatMap(x => x)
    import sparkSession.implicits._
    lineFrameList.toDF("line_name", "flow").createTempView("line_table")
    val sumSql = "select line_name,sum(flow) flow " +
      "from line_table " +
      "group by line_name"
    sparkSession.sql(sumSql)
  }

  private def lineRddOpt(lineMap: RDD[mutable.Buffer[LineWithFlow]]) = {
    val lineNameFlowRdd = lineMap.map(x => {
      val lineNameWithFlow = new util.HashMap[String, Double]()
      for (lineInfo <- x) {
        val lines = lineInfo.getLines.asScala
        val passengers: lang.Double = lineInfo.getPassengers
        for (line <- lines) {
          if (lineNameWithFlow.containsKey(line)) {
            val flow: Double = lineNameWithFlow.get(line)
            lineNameWithFlow.put(line, flow + passengers)
          } else {
            lineNameWithFlow.put(line, passengers)
          }
        }
      }
      lineNameWithFlow.asScala
    }
    )
    lineNameFlowRdd
  }
}

object LineFlow {
  def lineResult(associationMap: SectionAssociationMap, logitComputeResult: SimpleLogitResult): mutable.Buffer[LineWithFlow] = {
    val lineWithFlows: util.List[LineWithFlow] = SectionComputeService.getLineLogitResult(logitComputeResult, associationMap)
    lineWithFlows.asScala
  }
}
