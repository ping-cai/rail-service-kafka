package flowdistribute;

import kspcalculation.Path;

import java.io.Serializable;
import java.util.Map;

public class SimpleLogitResult implements Serializable {
    private OdWithTime odWithTime;
    private Map<Path, Double> logitResult;

    public SimpleLogitResult(OdWithTime odWithTime, Map<Path, Double> logitResult) {
        this.odWithTime = odWithTime;
        this.logitResult = logitResult;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }

    public Map<Path, Double> getLogitResult() {
        return logitResult;
    }
}
