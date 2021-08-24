package save

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait SaveRdd[T] extends Serializable {
  def saveByRdd(resultRdd: RDD[T], sparkSession: SparkSession): Unit
}
