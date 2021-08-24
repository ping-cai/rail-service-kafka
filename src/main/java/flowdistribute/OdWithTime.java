package flowdistribute;

import utils.DateExtendUtil;

import java.io.Serializable;

public class OdWithTime extends OdData implements Serializable {
    private String inTime;
    private String outTime;
    private Double passengers = 1.0;

    public OdWithTime(String inId, String outId, String inTime) {
        super(inId, outId);
        this.inTime = inTime;
    }

    public OdWithTime(String inId, String outId, String inTime, Double passengers) {
        super(inId, outId);
        this.inTime = inTime;
        this.passengers = passengers;
    }

    public OdWithTime(String inId, String outId, String inTime, String outTime) {
        super(inId, outId);
        this.inTime = inTime;
        this.outTime = outTime;
    }

    public OdWithTime(String inId, String outId, String inTime, String outTime, Double passengers) {
        super(inId, outId);
        this.inTime = inTime;
        this.outTime = outTime;
        this.passengers = passengers;
    }

    public void setPassengers(Double passengers) {
        this.passengers = passengers;
    }

    public Double getPassengers() {
        return passengers;
    }

    public OdWithTime(String inId, String outId) {
        super(inId, outId);
    }

    public String getInTime() {
        return inTime;
    }

    public String getOutTime() {
        return outTime;
    }

    @Override
    public String toString() {
        return "OdWithTime{" +
                "inId='" + getInId() + '\'' +
                "outId='" + getOutId() + '\'' +
                "inTime='" + inTime + '\'' +
                ", outTime='" + outTime + '\'' +
                ", passengers=" + passengers +
                '}';
    }

    public Integer getOdDiffMinutes() {
        return DateExtendUtil.timeDifference(inTime, outTime, DateExtendUtil.MINUTE);
    }
}
