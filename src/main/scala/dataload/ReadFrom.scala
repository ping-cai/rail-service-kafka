package dataload

import org.apache.spark.sql.DataFrame

trait ReadFrom[T] extends Serializable {
  def read(param:T):DataFrame
}
