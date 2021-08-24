package distribution

import java.{lang, util}
import domain.Section
import kspcalculation.{Edge, Path}
import utils.TimeKey

trait Distribute extends Serializable {
  def distribute(logitResult: java.util.Map[Path, Double],
                 sectionTravelGraph: util.Map[Section, Int],
                 sectionWithDirectionMap: java.util.Map[Section, String],
                 startTime: String,
                 timeInterval: Int,
                 dynamic: DynamicResult): DynamicResult
}

object Distribute {
  //  /**
  //    *
  //    * @param stationFlow 某时段的车站流量表
  //    * @param flow        将要叠加的流量，此流量会叠加到该时段车站流量表上
  //    * @param edge        边集，用于判断应该叠加的车站是否为换乘站，判断进站和出站
  //    * @param timeKey     时段Key
  //    * @return
  //    */
  //  def updateTimeStationFlow(stationFlow: util.Map[TimeKey, util.Map[StationWithType, lang.Double]], flow: Double, edge: Edge, timeKey: TimeKey) = {
  //    val departure = new StationWithType(edge.getFromNode, "out")
  //    val arrival = new StationWithType(edge.getToNode, "in")
  //    //          检查前时段的数据是否包含当前时段
  //    if (stationFlow.containsKey(timeKey)) {
  //      val oldTimeStationFlow = stationFlow.get(timeKey)
  //      //            如果有前时段出站的该车站的客流
  //      if (oldTimeStationFlow.containsKey(departure)) {
  //        val oldDepartureFlow = oldTimeStationFlow.get(departure)
  //        oldTimeStationFlow.put(departure, flow + oldDepartureFlow)
  //      } else {
  //        //              如果前时段没有
  //        oldTimeStationFlow.put(departure, flow)
  //      }
  //      //            如果有前时段进站的该车站的客流
  //      if (oldTimeStationFlow.containsKey(arrival)) {
  //        val oldArrivalFlow = oldTimeStationFlow.get(arrival)
  //        oldTimeStationFlow.put(departure, flow + oldArrivalFlow)
  //      } else {
  //        //              如果前时段没有
  //        oldTimeStationFlow.put(departure, flow)
  //      }
  //    } else {
  //      //        如果不包含
  //      val newTimeStationFlow = new util.HashMap[StationWithType, lang.Double]()
  //      newTimeStationFlow.put(departure, flow)
  //      newTimeStationFlow.put(arrival, flow)
  //      //            添加时段数据，为时段增量
  //      stationFlow.put(timeKey, newTimeStationFlow)
  //    }
  //  }

  /**
    *
    * @param stationFlow 某时段的车站流量表
    * @param flow        将要叠加的流量，此流量会叠加到该时段车站流量表上
    * @param edge        边集，用于判断应该叠加的车站是否为换乘站，判断进站和出站
    * @param timeKey     时段Key
    * @return
    */
  def updateTimeStationFlow(stationFlow: util.Map[TimeKey, util.Map[StationWithType, lang.Double]], flow: Double,
                            stationWithType: StationWithType, timeKey: TimeKey) = {
    //    如果存在有当前时段
    if (stationFlow.containsKey(timeKey)) {
      val stationWithTypeMap = stationFlow.get(timeKey)
      //      并且还存在当前时段的进出站记录
      if (stationWithTypeMap.containsKey(stationWithType)) {
        val oldFlow = stationWithTypeMap.get(stationWithType)
        stationWithTypeMap.put(stationWithType, oldFlow + flow)
      } else {
        //        如果不存在那么就添加
        stationWithTypeMap.put(stationWithType, flow)
      }
    } else {
      //      不存在当前时段，那么肯定不存在进出站记录
      val newStationWithTypeMap = new util.HashMap[StationWithType, lang.Double]()
      newStationWithTypeMap.put(stationWithType, flow)
      stationFlow.put(timeKey, newStationWithTypeMap)
    }
  }


  /**
    *
    * @param transferFlow 某时段的换乘流量表
    * @param flow         将要叠加的流量
    * @param edges        所有边集的集合
    * @param edgeNum      换乘的边集位置
    * @param section      换乘虚拟区间
    * @param timeKey      时段Key
    * @return
    */
  def updateTimeTransferFlow(transferFlow: util.Map[TimeKey, util.Map[TransferWithDirection, lang.Double]], flow: Double, edges: util.LinkedList[Edge],
                             edgeNum: Int, section: Section, timeKey: TimeKey, sectionWithDirectionMap: util.Map[Section, String]) = {
    //    这里容易指针越界，需要判定是否为首尾换乘的站
    val lastSection = Section.createSectionByEdge(edges.get(edgeNum - 1))
    val nextSection = Section.createSectionByEdge(edges.get(edgeNum + 1))
    val lastDirection = sectionWithDirectionMap.get(lastSection)
    val nextDirection = sectionWithDirectionMap.get(nextSection)
    val transferWithDirection = new TransferWithDirection(section, lastDirection + nextDirection)
    if (transferFlow.containsKey(timeKey)) {
      val oldTransferFlow = transferFlow.get(timeKey)
      //            判断已经出现的时段并且包含的换乘区间是否包含当前时刻的应该加入的换乘区间
      if (oldTransferFlow.containsKey(transferWithDirection)) {
        //              当前换乘区间的换乘人数
        val oldFlow = oldTransferFlow.get(transferWithDirection)
        //              更新人数
        oldTransferFlow.put(transferWithDirection, oldFlow + flow)
      } else {
        //              直接新增
        oldTransferFlow.put(transferWithDirection, flow)
      }
    } else {
      //              没有出现的时段的换乘区间应该初始化新的Map进行添加
      val newTransferFlow: util.Map[TransferWithDirection, lang.Double] = new util.HashMap[TransferWithDirection, lang.Double]()
      newTransferFlow.put(transferWithDirection, flow)
      transferFlow.put(timeKey, newTransferFlow)
    }

  }

  /**
    *
    * @param sectionTraffic 某时段的区间流量表
    * @param flow           将要叠加的流量
    * @param timeKey        时段Key
    * @param section        区间
    * @return
    */
  def updateTimeSectionFlow(sectionTraffic: util.Map[TimeKey, util.Map[Section, lang.Double]], flow: Double, timeKey: TimeKey, section: Section) = {
    //          判断是否有这个时间段
    if (sectionTraffic.containsKey(timeKey)) {
      val oldTimeSectionFlow = sectionTraffic.get(timeKey)
      //            判断这个时间段是否有这个区间
      if (oldTimeSectionFlow.containsKey(section)) {
        val oldFlow = oldTimeSectionFlow.get(section)
        oldTimeSectionFlow.put(section, oldFlow + flow)
      } else {
        //              这个时间段没有这个区间的流量，对应初始化新的区间流量
        oldTimeSectionFlow.put(section, flow)
      }
    } else {
      //            没有这个时间段，那么初始化这个时间段及其新的区间流量
      val newSectionFlow = new util.HashMap[Section, lang.Double]()
      newSectionFlow.put(section, flow)
      sectionTraffic.put(timeKey, newSectionFlow)
    }
    //最后时间一定要进行叠加更新
  }

}