package dataload.back

import java.util.Properties

import conf.DynamicConf
import domain.Section
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class BackSectionLoad extends Serializable {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  val sectionSeqMap: mutable.HashMap[Int, mutable.HashSet[Section]] = mutable.HashMap[Int, mutable.HashSet[Section]]()
  private val url: String = DynamicConf.localhostUrl
  private val prop: Properties = new Properties() {
    put("user", DynamicConf.localhostUser)
    put("password", DynamicConf.localhostPassword)
  }
  private val backPushTable: String = DynamicConf.backPushTable
  private val sectionTable: String = DynamicConf.sectionTable
  init()

  def init(): Unit = {
    val sparkSession = SparkSession.builder().appName("BackSectionLoad").getOrCreate()
    log.info("start BackSectionLoad！two table will be Load,Those are {}", backPushTable)
    val sectionBackPushFrame = sparkSession.read.jdbc(url, backPushTable, prop)
    val sectionFrame = sparkSession.read.jdbc(url, sectionTable, prop).select("QJ_ID", "CZ1_ID", "CZ2_ID")
    sectionBackPushFrame.createOrReplaceTempView("back_push_table")
    sectionFrame.createOrReplaceTempView("section_table")
    val resultFrame = sparkSession.sql(
      """
select CZ1_ID,CZ2_ID,QD_ID from back_push_table origin
join section_table target on origin.SECTION_ID = target.QJ_ID
      """.stripMargin)
    resultFrame.persist()
    resultFrame.collect().foreach(x => {
      val inId = x.getDecimal(0).intValue().toString
      val outId = x.getDecimal(1).intValue().toString
      val seq = x.getDecimal(2).intValue()
      if (sectionSeqMap.contains(seq)) {
        val sectionSet = sectionSeqMap(seq)
        sectionSet.add(new Section(inId, outId))
      } else {
        val sectionSet = mutable.HashSet[Section]()
        sectionSet.add(new Section(inId, outId))
        sectionSeqMap.put(seq, sectionSet)
      }
    })
  }
}

object BackSectionLoad {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").getOrCreate()
    val backSectionLoad = new BackSectionLoad
    backSectionLoad.sectionSeqMap.foreach(x=>println(x))
  }
}