package flowreduce;

import java.io.Serializable;
import java.util.Map;

public class SimpleSectionMap implements Serializable {
    private Map<SimpleSection, Double> sectionFlowMap;

    public SimpleSectionMap(Map<SimpleSection, Double> sectionFlowMap) {
        this.sectionFlowMap = sectionFlowMap;
    }

    public Map<SimpleSection, Double> getSectionFlowMap() {
        return sectionFlowMap;
    }
}
