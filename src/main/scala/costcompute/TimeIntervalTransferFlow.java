package costcompute;

import distribution.TransferWithDirection;
import utils.TimeKey;

import java.io.Serializable;
import java.util.Map;

public class TimeIntervalTransferFlow implements Serializable {
    private Map<TimeKey, Map<TransferWithDirection, Double>> TimeIntervalTransferFlow;

    public TimeIntervalTransferFlow(Map<TimeKey, Map<TransferWithDirection, Double>> timeIntervalTransferFlow) {
        TimeIntervalTransferFlow = timeIntervalTransferFlow;
    }

    public Map<TimeKey, Map<TransferWithDirection, Double>> getTimeIntervalTransferFlow() {
        return TimeIntervalTransferFlow;
    }
}
