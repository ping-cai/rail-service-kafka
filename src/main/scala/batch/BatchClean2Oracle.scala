package batch

import java.util.Properties

import conf.DynamicConf
import org.apache.spark.sql.DataFrame

class BatchClean2Oracle extends Serializable {
  private val url: String = DynamicConf.localhostUrl
  val prop: Properties = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }

  def clean(dataFrame: DataFrame): Unit = {
    dataFrame.createOrReplaceTempView("afc_origin")

  }
}
