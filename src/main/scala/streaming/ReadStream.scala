package streaming

import org.apache.spark.sql.DataFrame

trait ReadStream extends Serializable {
  def readStream(): DataFrame
}
