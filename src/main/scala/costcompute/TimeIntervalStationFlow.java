package costcompute;

import distribution.StationWithType;
import utils.TimeKey;

import java.io.Serializable;
import java.util.Map;

public class TimeIntervalStationFlow implements Serializable {
    private Map<TimeKey, Map<StationWithType, Double>> timeStationTraffic;

    public TimeIntervalStationFlow(Map<TimeKey, Map<StationWithType, Double>> timeStationTraffic) {
        this.timeStationTraffic = timeStationTraffic;
    }

    public Map<TimeKey, Map<StationWithType, Double>> getTimeStationTraffic() {
        return timeStationTraffic;
    }
}
