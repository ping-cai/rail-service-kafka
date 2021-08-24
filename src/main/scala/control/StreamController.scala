package control

import org.apache.spark.sql.SparkSession
import streaming.OdPairStream

class StreamController(sparkSession: SparkSession) {
  def startOdPair(): Unit = {
    val odPairStream = new OdPairStream(sparkSession)
    odPairStream.compute()
  }
}

object StreamController {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    val streamController = new StreamController(sparkSession)
    streamController.startOdPair()
  }
}
