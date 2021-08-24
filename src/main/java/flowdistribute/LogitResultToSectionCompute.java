package flowdistribute;

import kspcalculation.DirectedPath;

import java.io.Serializable;
import java.util.Map;

public class LogitResultToSectionCompute implements Serializable {
    private Map<DirectedPath, Double> logitResult;
    private OdWithTime odWithTime;

    public LogitResultToSectionCompute(Map<DirectedPath, Double> logitResult, OdWithTime odWithTime) {
        this.logitResult = logitResult;
        this.odWithTime = odWithTime;
    }

    public Map<DirectedPath, Double> getLogitResult() {
        return logitResult;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }
}
