package service

import org.apache.spark.sql

trait DivideTime {
  def divide(data: sql.DataFrame): sql.DataFrame
}
