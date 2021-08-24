package flowreduce;

import scala.Serializable;
import utils.TimeKey;


public class LineSave extends TimeKey implements Serializable {
    private String lineName;
    private float flow;

    public LineSave(TimeKey timeKey, String lineName, float flow) {
        super(timeKey.getStartTime(), timeKey.getEndTime());
        this.lineName = lineName;
        this.flow = flow;
    }

    public LineSave(String startTime, String endTime) {
        super(startTime, endTime);
    }

    public LineSave(String startTime, String endTime, String lineName, float flow) {
        super(startTime, endTime);
        this.lineName = lineName;
        this.flow = flow;
    }

    public String getLineName() {
        return lineName;
    }

    public float getFlow() {
        return flow;
    }
}
