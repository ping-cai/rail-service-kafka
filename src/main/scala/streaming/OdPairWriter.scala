package streaming

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.spark.sql.{ForeachWriter, Row}

class OdPairWriter(driver: String, url: String, username: String, password: String) extends ForeachWriter[Row] with Serializable {
  var connection: Connection = _
  var preparedStatement: PreparedStatement = _
  var tableName: String = _
  var sql = s"insert into OD_KALMAN(START_TIME,IN_ID,OUT_ID,PASSENGERS) value (?,?,?,?)"
  var batchCount = 0

  override def open(partitionId: Long, epochId: Long): Boolean = {
    connection = DriverManager.getConnection(url, username, password)
    preparedStatement = connection.prepareStatement(sql)
    connection != null && !connection.isClosed
  }

  override def process(value: Row): Unit = {
    val startTime = value.getTimestamp(0)
    setTableName(startTime)
    val inId = value.getString(1)
    val outId = value.getString(2)
    val passengers = value.getInt(3)
    preparedStatement.setTimestamp(1, startTime)
    preparedStatement.setString(2, inId)
    preparedStatement.setString(3, outId)
    preparedStatement.setInt(4, passengers)
    preparedStatement.addBatch()
    batchCount += 1
    if (batchCount > 1) {
      //      执行批量插入
      preparedStatement.executeBatch()
      //      提交事务
      connection.commit()
      //      数据位数归零
      batchCount = 0
    }
    println(s"插入数据成功！数据为$startTime $inId $outId $passengers ")
  }

  override def close(errorOrNull: Throwable): Unit = {
    preparedStatement.executeBatch()
    connection.commit()
    batchCount = 0
  }

  private def setTableName(startTime: Timestamp): Unit = {
    val date = startTime.toLocalDateTime
    val localDate = date.toLocalDate
    val year = localDate.getYear - 2000
    val month = localDate.getMonthValue
    val week = localDate.getDayOfMonth / 7
    tableName = s"KALMAN_OD_${year}_${month}_$week"
  }
}
