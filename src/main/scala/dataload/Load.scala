package dataload

import java.util.Properties

import conf.DynamicConf
import org.apache.spark.sql

trait Load extends Serializable {

  val prop = Load.prop

  def load(): sql.DataFrame
}

object Load {
  val url: String = DynamicConf.localhostUrl
  //  val url = "jdbc:oracle:thin:@192.168.1.153:1521:ORCL"
  val prop: Properties = new java.util.Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
}