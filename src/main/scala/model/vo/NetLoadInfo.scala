package model.vo

case class NetLoadInfo(RecordTime: String,
                       var StationLoads: List[StationLoad], var SectionLoads: List[SectionLoad]) {

}
