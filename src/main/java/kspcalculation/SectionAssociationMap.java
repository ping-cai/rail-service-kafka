package kspcalculation;

import java.io.Serializable;
import java.util.Map;

public class SectionAssociationMap implements Serializable {
    private Map<String, SectionAllInfo> sectionAllInfoMap;

    public SectionAssociationMap(Map<String, SectionAllInfo> sectionAllInfoMap) {
        this.sectionAllInfoMap = sectionAllInfoMap;
    }

    public Map<String, SectionAllInfo> getSectionAllInfoMap() {
        return sectionAllInfoMap;
    }

    public SectionAllInfo getSectionInfo(Edge edge) {
        return sectionAllInfoMap.get(String.format("%s %s", edge.getFromNode(), edge.getToNode()));
    }

    public String getLineName(Edge edge) {
        return getSectionInfo(edge).getLineName();
    }
}
