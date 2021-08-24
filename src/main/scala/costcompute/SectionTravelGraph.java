package costcompute;

import domain.Section;

import java.io.Serializable;
import java.util.*;

/**
 * 区间运行图类
 *
 * @author ping
 */
public class SectionTravelGraph implements Serializable {
    private Map<Section, List<TravelTime>> sectionListMap;
    public Map<Section, Integer> sectionTravelMap;

    public static SectionTravelGraph createTravelGraph(List<SectionTravelTime> sectionTravelTimeList) {
        Map<Section, List<TravelTime>> travelTimeMap = new HashMap<>();
        for (SectionTravelTime sectionTravelTime : sectionTravelTimeList) {
            Section section = sectionTravelTime.getSection();
            TravelTime travelTime = sectionTravelTime.getTravelTime();
            if (!travelTimeMap.containsKey(section)) {
                List<TravelTime> timeList = new ArrayList<>();
                timeList.add(travelTime);
                travelTimeMap.put(section, timeList);
            } else {
                List<TravelTime> travelTimeList = travelTimeMap.get(section);
                travelTimeList.add(travelTime);
                travelTimeMap.put(section, travelTimeList);
            }
        }
        return new SectionTravelGraph(travelTimeMap);
    }


    public SectionTravelGraph(Map<Section, List<TravelTime>> sectionListMap) {
        this.sectionListMap = sectionListMap;
        initSectionTravelMap(sectionListMap);
    }

    private void initSectionTravelMap(Map<Section, List<TravelTime>> sectionListMap) {
        HashMap<Section, Integer> sectionIntegerHashMap = new HashMap<>();
        for (Map.Entry<Section, List<TravelTime>> entry : sectionListMap.entrySet()) {
            List<TravelTime> travelTimeList = entry.getValue();
//            排序区间运行时间
            travelTimeList.sort(Comparator.comparingInt(TravelTime::getSeconds));
            TravelTime travelTime = travelTimeList.get(travelTimeList.size() / 2);
            sectionIntegerHashMap.put(entry.getKey(), travelTime.getSeconds());
        }
        sectionTravelMap = sectionIntegerHashMap;
    }

    public Map<Section, List<TravelTime>> getSectionListMap() {
        return sectionListMap;
    }

    public Integer getTravelTime(Section section) {
        return sectionTravelMap.get(section);
    }

    @Override
    public String toString() {
        return "SectionTravelGraph{" +
                "sectionListMap=" + sectionListMap +
                '}';
    }
}
