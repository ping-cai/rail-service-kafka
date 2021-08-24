package flowreduce;

import utils.TimeKey;

import java.io.Serializable;
import java.util.Map;

public class SectionResult implements Serializable {
    private Map<TimeKey, Map<SimpleSection, Double>> simpleSectionMap;
    private Map<TimeKey, LineResult> lineResultMap;
    private Map<TimeKey, Map<SimpleTransfer, Double>> simpleTransferMap;

    public SectionResult(Map<TimeKey, Map<SimpleSection, Double>> simpleSectionMap, Map<TimeKey, LineResult> lineResultMap) {
        this.simpleSectionMap = simpleSectionMap;
        this.lineResultMap = lineResultMap;
    }

    public SectionResult(Map<TimeKey, Map<SimpleSection, Double>> simpleSectionMap, Map<TimeKey, LineResult> lineResultMap, Map<TimeKey, Map<SimpleTransfer, Double>> simpleTransferMap) {
        this.simpleSectionMap = simpleSectionMap;
        this.lineResultMap = lineResultMap;
        this.simpleTransferMap = simpleTransferMap;
    }

    public Map<TimeKey, Map<SimpleTransfer, Double>> getSimpleTransferMap() {
        return simpleTransferMap;
    }

    public Map<TimeKey, Map<SimpleSection, Double>> getSimpleSectionMap() {
        return simpleSectionMap;
    }

    public Map<TimeKey, LineResult> getLineResultMap() {
        return lineResultMap;
    }

}
