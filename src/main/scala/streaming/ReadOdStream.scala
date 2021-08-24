package streaming

import conf.DynamicConf
import org.apache.spark.sql.functions.{explode, from_json}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class ReadOdStream(sparkSession: SparkSession) extends ReadStream {
  val dynamicParamConf: DynamicConf.type = DynamicConf
  val odListSchema: StructType = new StructType()
    .add("odList", DataTypes.createArrayType(DataTypes.StringType))
  val oWithOutDSchema: StructType = new StructType()
    .add("inId", DataTypes.StringType)
    .add("passengers", DataTypes.DoubleType)
    .add("inTime", DataTypes.StringType)
  val odSchema: StructType = new StructType()
    .add("outId", DataTypes.StringType)
    .add("oWithOutD", oWithOutDSchema)
  //    导入隐式转换
  import sparkSession.implicits._

  private val odListTopic = dynamicParamConf.topics(0)

  /**
    * 读取到来的完成OD集合数据
    *
    * @return 返回OD数据集
    */
  override def readStream(): DataFrame = {
    val kafkaStreaming = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", dynamicParamConf.brokers)
      .option("subscribe", odListTopic)
      .option("startingOffsets", "latest")
      .option("enable.auto.commit", "false")
      .option("auto.commit.interval.ms", "1000")
      .load()
    val kafkaData = kafkaStreaming.selectExpr("CAST(key AS STRING)", "CAST (value AS STRING) as json")
      .as[(String, String)]
      //      过滤json
      .filter(x => JSON.parseFull(x._2).isDefined)
      //      对应schema信息，列别名取为od
      .select(from_json($"json", schema = odListSchema).alias("odList"))
      .select(explode($"odList.odList"))
      .select(from_json($"col", schema = odSchema).alias("od"))
      .select("od.oWithOutD.inId", "od.outId", "od.oWithOutD.inTime", "od.oWithOutD.passengers")
    kafkaData
  }
}

object ReadOdStream {

}
