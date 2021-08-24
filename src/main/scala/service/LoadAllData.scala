package service

import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class LoadAllData extends Load with Serializable {
  override def load(): DataFrame = ???
}

object LoadAllData {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val data = new LoadAllData
    data.load()
  }
}
