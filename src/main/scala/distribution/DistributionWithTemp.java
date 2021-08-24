package distribution;

import java.io.Serializable;

public class DistributionWithTemp implements Serializable {
    private DistributionResult finalResult;
    private DistributionResult tempResult;

    public DistributionWithTemp(DistributionResult finalResult, DistributionResult tempResult) {
        this.finalResult = finalResult;
        this.tempResult = tempResult;
    }

    public DistributionResult getFinalResult() {
        return finalResult;
    }

    public DistributionResult getTempResult() {
        return tempResult;
    }

    @Override
    public String toString() {
        return "DistributionWithTemp{" +
                "finalResult=" + finalResult +
                ", tempResult=" + tempResult +
                '}';
    }
}
