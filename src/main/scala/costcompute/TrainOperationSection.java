package costcompute;

import domain.Section;

import java.io.Serializable;
import java.util.Map;

/**
 * 区间->列车对应集
 */
public class TrainOperationSection implements Serializable {
    private Map<Section, Train> sectionTrainMap;

    public TrainOperationSection(Map<Section, Train> sectionTrainMap) {
        this.sectionTrainMap = sectionTrainMap;
    }

    public Map<Section, Train> getSectionTrainMap() {
        return sectionTrainMap;
    }
}
