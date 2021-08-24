package save

import java.util.Properties

import conf.DynamicConf
import org.apache.spark.sql.SparkSession

trait Save[T] extends Serializable {
  def save(resultData: T, sparkSession: SparkSession)
}

object Save {
  val url: String = DynamicConf.localhostUrl
  val prop: Properties = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
  val HDFS_PREFIX: String = DynamicConf.hdfsNameSpace
}
