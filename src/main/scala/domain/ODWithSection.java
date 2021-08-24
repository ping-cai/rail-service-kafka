package domain;

import flowdistribute.OdWithTime;
import flowreduce.TimeSectionMap;

import java.io.Serializable;

/**
 * OD以及其分配过程中参数的区间流量（包含时间信息）
 */
public class ODWithSection implements Serializable {
    private OdWithTime odWithTime;
    private TimeSectionMap timeSectionMap;

    public ODWithSection(OdWithTime odWithTime, TimeSectionMap timeSectionMap) {
        this.odWithTime = odWithTime;
        this.timeSectionMap = timeSectionMap;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }

    public TimeSectionMap getTimeSectionMap() {
        return timeSectionMap;
    }
}
