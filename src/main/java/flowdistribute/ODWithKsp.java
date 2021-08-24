package flowdistribute;

import kspcalculation.DirectedPath;

import java.io.Serializable;
import java.util.List;

public class ODWithKsp implements Serializable {
    private List<DirectedPath> kspResult;
    private OdWithTime odWithTime;

    public ODWithKsp(List<DirectedPath> kspResult, OdWithTime odWithTime) {
        this.kspResult = kspResult;
        this.odWithTime = odWithTime;
    }

    public List<DirectedPath> getKspResult() {
        return kspResult;
    }

    public OdWithTime getOdWithTime() {
        return odWithTime;
    }
}
