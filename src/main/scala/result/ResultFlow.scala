package result

import flowdistribute.LineWithFlow
import flowreduce.SectionResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait ResultFlow extends Serializable {
  def getFlow(odTransFrame: DataFrame, rddTuple: RDD[(mutable.Buffer[LineWithFlow], SectionResult)]): sql.DataFrame
}
