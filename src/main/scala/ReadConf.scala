import com.typesafe.config.{Config, ConfigFactory}

object ReadConf extends Serializable {
  val conf: Config = ConfigFactory.load("kafka.conf")
  //  kafka.broker.list = "192.168.121.128:9092"
  //  kafka.localCheckpointLocation = "D:/Destop/checkpointLocation"
  //  kafka.onLineCheckpointLocation = "/home/pingcai/checkpointLocation"
  //  kafka.read.topic = "quickstart-event"
  //  kafka.write.topic = "quick-deploy"
  val brokers: String = conf.getString("kafka.broker.list")
  val localCheckpointLocation: String = conf.getString("kafka.localCheckpointLocation")
  val onLineCheckpointLocation: String = conf.getString("kafka.onLineCheckpointLocation")
  val readTopic: String = conf.getString("kafka.read.topic")
  val writeTopic: String = conf.getString("kafka.write.topic")
  
}
