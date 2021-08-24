package flowreduce;

import utils.TimeKey;

import java.io.Serializable;
import java.util.Map;

public class TimeSectionMap implements Serializable {
    private Map<TimeKey, Map<SimpleSection, Double>> timeKeyMapMap;

    public TimeSectionMap(Map<TimeKey, Map<SimpleSection, Double>> timeKeyMapMap) {
        this.timeKeyMapMap = timeKeyMapMap;
    }

    public Map<TimeKey, Map<SimpleSection, Double>> getTimeKeyMapMap() {
        return timeKeyMapMap;
    }
}
