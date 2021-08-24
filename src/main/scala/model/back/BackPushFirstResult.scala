package model.back

import java.sql.Timestamp

case class BackPushFirstResult(inId: String, outId: String, time: Timestamp, sectionSeq: Int, seqFlow: Double, totalFlow: Double) {

}
