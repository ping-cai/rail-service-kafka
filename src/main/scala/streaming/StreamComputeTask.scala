package streaming

import org.apache.spark.sql.SparkSession

class StreamComputeTask(sparkSession: SparkSession) extends StreamCompute {
  override def compute(): Unit = {
    val odPairStream = new OdPairStream(sparkSession)
    odPairStream.compute()
    val netWorkLoadStream = new NetWorkLoadStream(sparkSession)
    netWorkLoadStream.compute()
    val securityIndicatorsStream = new SecurityIndicatorsStream(sparkSession)
    securityIndicatorsStream.compute()
  }
}

object StreamComputeTask {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("StreamComputeTask").getOrCreate()
    val streamComputeTask = new StreamComputeTask(sparkSession)
    streamComputeTask.compute()
  }
}
