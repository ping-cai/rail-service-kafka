package distribution;

import costcompute.TimeIntervalTraffic;
import flowdistribute.OdWithTime;

import java.io.Serializable;

public class ODWithSectionResult implements Serializable {
    private OdWithTime odWithTime;
    private TimeIntervalTraffic tempSectionResult;

    public ODWithSectionResult(OdWithTime odWithTime, TimeIntervalTraffic tempSectionResult) {
        this.odWithTime = odWithTime;
        this.tempSectionResult = tempSectionResult;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }

    public TimeIntervalTraffic getTempSectionResult() {
        return tempSectionResult;
    }

    @Override
    public String toString() {
        return "ODWithSectionResult{" +
                "odWithTime=" + odWithTime +
                ", tempSectionResult=" + tempSectionResult +
                '}';
    }
}
