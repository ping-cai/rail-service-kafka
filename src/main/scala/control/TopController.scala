package control

import conf.DynamicConf
import org.apache.spark.sql.SparkSession

class TopController(sparkSession: SparkSession) extends Control {
  override def startup(controlInfo: ControlInfo): Unit = {
    val distributionType = controlInfo.getDistributionType
    if ("dynamic".equals(distributionType)) {
      val baseController = new BaseController(sparkSession)
      baseController.startup(controlInfo)
    } else {
      val staticController = new StaticController(sparkSession)
      staticController.startup(controlInfo)
    }
  }
}

object TopController {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    //    val controlInfo = topCommand(args)
    val odSource = DynamicConf.odCsvFile
    val controlInfo = new ControlInfo(odSource)
    val topController = new TopController(sparkSession)
    topController.startup(controlInfo)
  }

  //  def topCommand(args: Array[String]): ControlInfo = {
  //    /*
  //   od数据地址
  //   格式大致为 "hdfs://hacluster/od/data/diff.csv"
  //    */
  //    val odPath = args(0)
  //    //    分配类型
  //    /*
  //        线路分配结果路径
  //        格式大致为 "distributionType:static"
  //         */
  //    val distributionType = args(1).split(":")(1)
  //    /*
  //        线路分配结果路径
  //        格式大致为 "hdfs://hacluster/od/data/lineResult"
  //         */
  //    val kspNum = args(2).split(":")(1).toInt
  //
  //    val openShareFunction = args(3).split(":")(1).toBoolean
  //
  //    val controlInfo = new ControlInfo(odPath, distributionType, kspNum)
  //    controlInfo.setOpenShareFunction(openShareFunction)
  //    controlInfo
  //  }
}
