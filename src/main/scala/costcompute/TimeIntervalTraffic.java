package costcompute;

import domain.Section;
import utils.TimeKey;

import java.io.Serializable;
import java.util.Map;

/**
 * 时间段，区间->人数 Map集合
 */
public class TimeIntervalTraffic implements Serializable {
    //    每个时段的区间客流
    private Map<TimeKey, Map<Section, Double>> timeSectionTraffic;

    public TimeIntervalTraffic(Map<TimeKey, Map<Section, Double>> timeSectionTraffic) {
        this.timeSectionTraffic = timeSectionTraffic;
    }

    public Map<TimeKey, Map<Section, Double>> getTimeSectionTraffic() {
        return timeSectionTraffic;
    }

    @Override
    public String toString() {
        return "TimeIntervalTraffic{" +
                "timeSectionTraffic=" + timeSectionTraffic +
                '}';
    }
}
