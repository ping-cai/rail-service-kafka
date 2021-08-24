package flowdistribute;

import kspcalculation.DirectedPath;

import java.io.Serializable;
import java.util.Map;

public class LogitComputeResult implements Serializable {
    private OdWithTime odWithTime;
    private Map<DirectedPath, Double> logitResult;

    public LogitComputeResult(OdWithTime odWithTime, Map<DirectedPath, Double> logitResult) {
        this.odWithTime = odWithTime;
        this.logitResult = logitResult;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }

    public Map<DirectedPath, Double> getLogitResult() {
        return logitResult;
    }
}
