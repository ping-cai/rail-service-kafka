import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

class StructureStreamTest(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val servers: String = ReadConf.brokers
  val readTopic: String = ReadConf.readTopic
  val writeTopic: String = ReadConf.writeTopic
  val localCheckpointLocation: String = ReadConf.localCheckpointLocation
  val onLineCheckpointLocation: String = ReadConf.onLineCheckpointLocation

  def steamTest(): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", readTopic)
      .load()
    val streamResult = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val streamRead = streamResult.writeStream.format("console")
      .outputMode(OutputMode.Append()).start()
    val streamWrite = streamResult
      .toJSON
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("topic", writeTopic)
      .option("checkpointLocation", localCheckpointLocation)
      .outputMode(OutputMode.Append())
      .start()
    streamRead.awaitTermination()
    streamWrite.awaitTermination()
  }
}

object StructureStreamTest {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[2]").getOrCreate()
    val sparkSession = SparkSession.builder().appName("StructureStreamTest").getOrCreate()
    val structureStreamTest = new StructureStreamTest(sparkSession)
    structureStreamTest.steamTest()
  }
}