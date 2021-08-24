package model.back

import java.sql.Timestamp

case class BackPushSecondResult(inId: String, outId: String, time: Timestamp, sectionSeq: Int, notPassedSectionSeqPathId: String, pathFlow: Double) {

}
