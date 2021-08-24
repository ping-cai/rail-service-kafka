package distribution;

import flowdistribute.LineWithFlow;

public class DynamicResult {
    private DistributionResult distributionResult;
    private LineWithFlow lineWithFlow;

    public DynamicResult(DistributionResult distributionResult, LineWithFlow lineWithFlow) {
        this.distributionResult = distributionResult;
        this.lineWithFlow = lineWithFlow;
    }

    public DistributionResult getDistributionResult() {
        return distributionResult;
    }

    public LineWithFlow getLineWithFlow() {
        return lineWithFlow;
    }
}
