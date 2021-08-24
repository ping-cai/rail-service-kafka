package domain.dto

/**
  *
  * @param START_TIME       某个时段开始时间
  * @param END_TIME         某个时段结束时间
  * @param SECTION_ID       区间ID
  * @param START_STATION_ID 区间开始车站ID
  * @param END_STATION_ID   区间结束车站ID
  * @param PASGR_FLOW_QTTY  区间流量
  */
case class SectionDataFrame(START_TIME: String, END_TIME: String, SECTION_ID: Int, START_STATION_ID: String, END_STATION_ID: String, PASGR_FLOW_QTTY: Double)

case class LineDataFrame(lineName: String, flow: Float)

case class ODError(inId: String, outId: String)

case class SectionTravelToOracle(IN_ID: String, OUT_ID: String, TIME_SECONDS: Int, RATE: Double)

/**
  *
  * @param timeKeyStart 某个时段开始时间
  * @param timeKeyEnd   某个时段结束时间
  * @param sectionId    区间ID
  * @param startId      区间开始车站ID
  * @param endId        区间结束车站ID
  * @param origin       出发地
  * @param destination  目的地
  * @param inTime       出发地时间
  * @param outTime      到达时间
  * @param odShareFlow  分担率
  */
case class ODShare(timeKeyStart: String, timeKeyEnd: String,
                   sectionId: Int, startId: String, endId: String,
                   origin: String, destination: String, inTime: String, outTime: String,
                   odShareFlow: Double)

case class ODShareWithPassengers(timeKeyStart: String, timeKeyEnd: String,
                                 sectionId: Int, startId: String, endId: String,
                                 origin: String, destination: String, inTime: String, outTime: String,
                                 odShareFlow: Double, odPassengers: Double)

case class SectionResult(START_TIME: String, END_TIME: String, START_STATION_ID: String, END_STATION_ID: String, PASSENGERS: Double)

case class StationDataFrame(STATION_ID: String, START_TIME: String, END_TIME: String, ENTRY_QUATITY: Double, EXIT_QUATITY: Double, ENTRY_EXIT_QUATITY: Double)

/**
  *
  * @param XFER_STATION_ID  换乘站编号
  * @param TRAF_IN_LINE_ID  换入线路编号
  * @param TRAF_OUT_LINE_ID 换出线路编号
  * @param TRAF_SIN_SOUT    上进上出换乘量
  * @param TRAF_SIN_XOUT    上进下出换乘量
  * @param TRAF_XIN_SOUT    下进上出换乘量
  * @param TRAF_XIN_XOUT    下进下出换乘量
  */
case class TransferDataFrame(START_TIME: String, END_TIME: String, XFER_STATION_ID: String, TRAF_IN_LINE_ID: String, TRAF_OUT_LINE_ID: String,
                             TRAF_SIN_SOUT: Double, TRAF_SIN_XOUT: Double, TRAF_XIN_SOUT: Double, TRAF_XIN_XOUT: Double)

case class TransferResult(START_TIME: String, END_TIME: String, XFER_STATION_ID: String, TRANS_QUATITY: Double)