package dataload.base

import conf.DynamicConf
import dataload.Load
import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleTransferLoad() extends Load {
  private var stationTable: String = DynamicConf.stationTable
  private var url: String = Load.url

  def this(transferTable: String) {
    this()
    this.stationTable = transferTable
  }

  def this(transferTable: String, url: String) {
    this()
    this.stationTable = transferTable
    this.url = url
  }

  /**
    * 返回换乘的虚拟区间，也就是同一个站点的不同ID
    *
    * @return DataFrame
    */
  override def load(): DataFrame = {
    val sparkSession = SparkSession.builder()
      .appName("OracleTransferLoad")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val stationFrame = sparkSession.read.jdbc(url, stationTable, prop)
    stationFrame.createOrReplaceTempView("yxtStation")
    val transferStationSql = "SELECT\n" +
      "	origin.CZ_ID in_id,\n" +
      "	target.CZ_ID out_id \n" +
      "FROM\n" +
      "	(\n" +
      "	SELECT\n" +
      "		CZ_ID,\n" +
      "		CZ_NAME \n" +
      "	FROM\n" +
      "		yxtStation \n" +
      "	WHERE\n" +
      "	CZ_NAME IN ( SELECT CZ_NAME FROM yxtStation GROUP BY CZ_NAME HAVING count(*)>=2)) origin\n" +
      "	JOIN yxtStation target ON origin.CZ_NAME = target.CZ_NAME \n" +
      "WHERE\n" +
      "	origin.CZ_ID <> target.CZ_ID \n" +
      "GROUP BY\n" +
      "	origin.CZ_ID,\n" +
      "	target.CZ_ID \n" +
      "ORDER BY\n" +
      "	origin.CZ_ID"
    val transferStation = sparkSession.sql(transferStationSql)
    transferStation.createOrReplaceTempView("transfer_station")
    val allTransferInfo = "select in_id,out_id,target1.LJM out_line,target2.LJM in_line from transfer_station origin " +
      "join yxtStation target1 on origin.in_id=target1.CZ_ID " +
      "join yxtStation target2 on origin.out_id=target2.CZ_ID "
    val transferFrame = sparkSession.sql(allTransferInfo)
    transferFrame
  }

}

object OracleTransferLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val sectionLoad = new OracleSectionLoad()
    val transferLoad = new OracleTransferLoad()
    transferLoad.load().show(1000)
  }
}
