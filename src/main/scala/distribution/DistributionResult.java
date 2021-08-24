package distribution;

import costcompute.TimeIntervalStationFlow;
import costcompute.TimeIntervalTraffic;
import costcompute.TimeIntervalTransferFlow;

import java.io.Serializable;

public class DistributionResult implements Serializable {
    private TimeIntervalTraffic timeIntervalTraffic;
    private TimeIntervalStationFlow timeIntervalStationFlow;
    private TimeIntervalTransferFlow timeIntervalTransferFlow;

    public DistributionResult(TimeIntervalTraffic timeIntervalTraffic) {
        this.timeIntervalTraffic = timeIntervalTraffic;
    }

    public DistributionResult(TimeIntervalTraffic timeIntervalTraffic, TimeIntervalStationFlow timeIntervalStationFlow) {
        this.timeIntervalTraffic = timeIntervalTraffic;
        this.timeIntervalStationFlow = timeIntervalStationFlow;
    }

    public DistributionResult(TimeIntervalTraffic timeIntervalTraffic, TimeIntervalStationFlow timeIntervalStationFlow, TimeIntervalTransferFlow timeIntervalTransferFlow) {
        this.timeIntervalTraffic = timeIntervalTraffic;
        this.timeIntervalStationFlow = timeIntervalStationFlow;
        this.timeIntervalTransferFlow = timeIntervalTransferFlow;
    }

    public TimeIntervalTraffic getTimeIntervalTraffic() {
        return timeIntervalTraffic;
    }

    public TimeIntervalStationFlow getTimeIntervalStationFlow() {
        return timeIntervalStationFlow;
    }

    public TimeIntervalTransferFlow getTimeIntervalTransferFlow() {
        return timeIntervalTransferFlow;
    }

    @Override
    public String toString() {
        return "DistributionResult{" +
                "timeIntervalTraffic=" + timeIntervalTraffic +
                ", timeIntervalStationFlow=" + timeIntervalStationFlow +
                ", timeIntervalTransferFlow=" + timeIntervalTransferFlow +
                '}';
    }
}
