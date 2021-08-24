package flowreduce;

import java.io.Serializable;
import java.util.Map;

public class LineResult implements Serializable {
    private Map<String, Double> lineFlowMap;

    public LineResult(Map<String, Double> lineFlowMap) {
        this.lineFlowMap = lineFlowMap;
    }

    public Map<String, Double> getLineFlowMap() {
        return lineFlowMap;
    }

    @Override
    public String toString() {
        return "线路结果{" +
                "线路集合=" + lineFlowMap +
                '}';
    }
}
