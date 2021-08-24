package dataload.base

import conf.DynamicConf
import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleSectionTrain extends Load {
  private var sectionTable: String = DynamicConf.sectionTable
  private var trainTable: String = DynamicConf.trainTable
  private var url: String = Load.url

  def this(sectionTable: String, trainTable: String, url: String) {
    this()
    this.sectionTable = sectionTable
    this.trainTable = trainTable
    this.url = url
  }

  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder().appName("OracleSectionTrain").getOrCreate()
    val sectionFrame = sparkSession.read.jdbc(url, sectionTable, prop)
    sectionFrame.createTempView("section_table")
    val trainFrame = sparkSession.read.jdbc(url, trainTable, prop)
    trainFrame.createTempView("train_table")
    val getSectionTrain = "SELECT origin.CZ1_ID,origin.CZ2_ID,origin.QJ_LJM,target.seat,target.MAX_CAPACITY " +
      s"FROM section_table origin " +
      s"JOIN train_table target ON origin.QJ_LJM = target.LINE_NAME"
    val sectionWithTrainFrame = sparkSession.sql(getSectionTrain)
    sectionWithTrainFrame
  }
}

object OracleSectionTrain {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sectionTrain = new OracleSectionTrain()
    sectionTrain.load().show()
  }
}