package kspcalculation;

import flowdistribute.OdWithTime;

import java.io.Serializable;
import java.util.Map;

public class KPathResult implements Serializable {
    private OdWithTime odWithTime;
    private Map<Path, Integer> pathWithTransferNum;

    public KPathResult(OdWithTime odWithTime, Map<Path, Integer> pathWithTransferNum) {
        this.odWithTime = odWithTime;
        this.pathWithTransferNum = pathWithTransferNum;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }

    public Map<Path, Integer> getPathWithTransferNum() {
        return pathWithTransferNum;
    }

}
