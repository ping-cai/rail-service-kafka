package utils

import java.sql.{Connection, DriverManager}
import java.util.concurrent.LinkedBlockingQueue

import conf.DynamicConf


object ConnectionPool extends Serializable {
  private val conf: DynamicConf.type = DynamicConf
  var driver: String = "oracle.jdbc.driver.OracleDriver"
  var url: String = conf.localhostUrl
  var poolSize: Int = 10
  Class.forName(driver)
  private val username: String = conf.localhostUser
  private val password: String = conf.localhostPassword
  @transient
  private val pool: LinkedBlockingQueue[Connection] = new LinkedBlockingQueue[Connection](poolSize + 1)
  //  初始化连接池对象
  Range(0, poolSize).foreach(_ => {
    val connection = DriverManager.getConnection(url, username, password)
    pool.put(connection)
  })

  /**
    * 可能会有死锁问题
    *
    * @return
    */
  def getConnection: Connection = {
    pool.take()
  }

  def returnConnect(conn: Connection): Unit = {
    pool.put(conn)
  }
}
