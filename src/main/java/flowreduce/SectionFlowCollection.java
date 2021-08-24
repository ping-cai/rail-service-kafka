package flowreduce;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SectionFlowCollection implements Serializable {
    private String startTime;
    private String endTime;
    private List<SimpleSection> simpleSectionList;
    private Map<String,Double> lineFlowMap;

    public SectionFlowCollection(String startTime, String endTime, List<SimpleSection> simpleSectionList, Map<String, Double> lineFlowMap) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.simpleSectionList = simpleSectionList;
        this.lineFlowMap = lineFlowMap;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public List<SimpleSection> getSimpleSectionList() {
        return simpleSectionList;
    }

    public Map<String, Double> getLineFlowMap() {
        return lineFlowMap;
    }
}
