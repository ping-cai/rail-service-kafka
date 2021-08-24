package result

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Flow[T] extends Serializable {
  def getFlow(sparkSession: SparkSession, rddTuple: RDD[T]): DataFrame
}
