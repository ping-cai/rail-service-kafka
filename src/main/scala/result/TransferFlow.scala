package result

import java.{lang, util}

import flowdistribute.LineWithFlow
import flowreduce.{SectionResult, SimpleTransfer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.TimeKey

import scala.collection.mutable

class TransferFlow extends ResultFlow {
  override def getFlow(odTransFrame: DataFrame, rddTuple: RDD[(mutable.Buffer[LineWithFlow], SectionResult)]): DataFrame = {
    val sectionResult = rddTuple.map(x => x._2)
    sectionResult.map(sectionResult => {
      val map: util.Map[TimeKey, util.Map[SimpleTransfer, lang.Double]] = sectionResult.getSimpleTransferMap
    })
    null
  }

  private def transferRddOpt(odTransFrame: DataFrame, transferWithTimeMap: util.Map[TimeKey, util.Map[SimpleTransfer, lang.Double]]): Unit = {
    transferWithTimeMap.forEach((timeKey, transferMap) => {
      timeKey
      transferMap.forEach((simpleTransfer, flow) => {
        val inId = simpleTransfer.getInId
        simpleTransfer.getOutId
      })
    })

  }
}

object TransferFlow {
  //  测试是否会重复计算
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val testList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val IntRdd = sc.makeRDD(testList)
    val addOneRdd = IntRdd.map(x => x + 1)
    val addRdd = addOneRdd.map(x => x * 2)
  }
}