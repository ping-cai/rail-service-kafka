package conf

import com.typesafe.config.{Config, ConfigFactory}

object DynamicConf extends Serializable {
  //  val conf: Config = ConfigFactory.load("prod.conf")
  //  val conf: Config = ConfigFactory.load("dev.conf")
  //    val conf: Config = ConfigFactory.load("tonghao.conf")
  //  val conf: Config = ConfigFactory.load("kafka.conf")
  //  val conf: Config = ConfigFactory.load("chongqing.conf")
  val conf: Config = ConfigFactory.load("test.conf")
  val topics: Array[String] = conf.getString("kafka.topic").split(",")
  val groupId: String = conf.getString("kafka.group.id")
  val brokers: String = conf.getString("kafka.broker.list")
  val kafkaCheckPoint: String = conf.getString("kafka.checkPoint")
  val odCsvFile: String = conf.getString("od.csv.file")
  val pathNum: Int = conf.getInt("dynamic.streaming.pathNum")
  val timeInterval: Int = conf.getInt("dynamic.streaming.timeInterval")
  val alpha: Double = conf.getDouble("dynamic.streaming.alpha")
  val beta: Double = conf.getDouble("dynamic.streaming.beta")
  val theta: Double = conf.getDouble("dynamic.streaming.theta")

  //  分配方式
  val distributionType: String = conf.getString("distribution.type")
  // 是否开启OD反推功能
  val openShareFunction: Boolean = conf.getBoolean("open.share.function")
  // 是否调整区间权值
  val openSectionAdjust: Boolean = conf.getBoolean("open.section.adjust")
  //  // 区间权值调整为,原有基础上增加x
  //  val sectionWeightAdd: Int = conf.getInt("section.weight.add")
  //  HDFS集群地址
  val hdfsNameSpace: String = conf.getString("hdfs.namespace")
  //  换乘时间,单位秒
  val transferTime: Double = conf.getDouble("dynamic.streaming.transfer.time") * 60
  //  停站时间，单位秒
  val stationStopTime: Double = conf.getDouble("dynamic.streaming.station.stop.time") * 60
  //  换乘惩罚时间,单位分钟
  val transferPenalty: Double = conf.getDouble("dynamic.streaming.transfer.penalty")
  //  乘车时间换算,单位分钟
  val sectionWeightRate: Double = conf.getDouble("dynamic.streaming.section.weight.rate")
  //开启路径约束
  val openPathConstraint: Boolean = conf.getBoolean("open.path.constraint")
  // 出行费用放大系数
  val travelExpenseAmplification: Double = conf.getDouble("travel.expense.amplification.factor")
  // 出行费用最大值约束
  val maximumTravelCost: Double = conf.getDouble("maximum.travel.cost")
  // 换乘次数偏离约束
  val transferTimesDeviation: Int = conf.getInt("transfer.times.deviation")
  //oracleConfig
  val localhostUrl: String = conf.getString("oracle.localhost.url")
  val localhostUser: String = conf.getString("oracle.localhost.user")
  val localhostPassword: String = conf.getString("oracle.localhost.password")
  val afcStationTable: String = conf.getString("oracle.afc.station.table")
  val stationTable: String = conf.getString("oracle.station.table")
  val sectionTravelTable: String = conf.getString("oracle.section.travel.table")
  val sectionTable: String = conf.getString("oracle.section.table")
  val trainTable: String = conf.getString("oracle.train.table")
  // 区间额外权值表
  val sectionExtraTable: String = conf.getString("oracle.section.extra.table")
  val stationDelay: String = conf.getString("dynamic.streaming.timeWithStationFrame.delayThreshold")
  val sectionDelay: String = conf.getString("dynamic.streaming.timeWithSectionFrame.delayThreshold")
  val odPairDelay: String = conf.getString("od.pair.delayThreshold")
  val windowDuration: String = conf.getString("dynamic.streaming.windowDuration")
  val slideDuration: String = conf.getString("dynamic.streaming.slideDuration")
  val backPushTable: String = conf.getString("oracle.back.push.table")
  //  DataClean
  val afcCleanTopic: Array[String] = conf.getString("kafka.streaming.afc.clean.topic").split(",")

}
