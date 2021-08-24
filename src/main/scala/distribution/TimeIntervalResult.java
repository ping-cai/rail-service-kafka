package distribution;

import costcompute.TimeIntervalStationFlow;
import costcompute.TimeIntervalTraffic;
import costcompute.TimeIntervalTransferFlow;

import java.io.Serializable;

public interface TimeIntervalResult extends Serializable {
    TimeIntervalTraffic getTimeIntervalTraffic();

    TimeIntervalStationFlow getTimeIntervalStationFlow();

    TimeIntervalTransferFlow getTimeIntervalTransferFlow();
}
