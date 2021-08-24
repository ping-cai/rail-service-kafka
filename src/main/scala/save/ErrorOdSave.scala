package save

import domain.dto.ODWithTimeScala
import flowdistribute.OdWithTime
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

class ErrorOdSave(errorOdTable: String) extends SaveRdd[OdWithTime] {
  override def saveByRdd(resultRdd: RDD[OdWithTime], sparkSession: SparkSession): Unit = {
    val odWithTimeScala = resultRdd.map(x => {
      val inTime = x.getInTime
      val outTime = x.getOutTime
      val inId = x.getInId
      val outId = x.getOutId
      val passengers = x.getPassengers
      ODWithTimeScala(inId, inTime, outId, outTime, passengers)
    })
    import sparkSession.implicits._
    val errorOdFrame = odWithTimeScala.toDF()
    errorOdFrame.write.mode(SaveMode.Append).jdbc(Save.url, errorOdTable, Save.prop)
  }
}
