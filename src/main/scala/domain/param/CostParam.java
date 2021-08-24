package domain.param;

import conf.DynamicConf;

import java.io.Serializable;

public class CostParam implements Serializable {
    //  换乘时间,单位秒
    public static final double TRANSFER_TIME = DynamicConf.transferTime();
    //  停站时间，单位秒
    public static final double STOP_STATION_TIME = DynamicConf.stationStopTime();
    //  换乘惩罚时间,单位分钟
    public static final double TRANSFER_PENALTY = DynamicConf.transferPenalty();
    //  乘车时间换算,单位分钟
    public static final double SECTION_WEIGHT_ADD_RATE = DynamicConf.sectionWeightRate();
    //  矫正参数α
    public static final double A = DynamicConf.alpha();
    //  矫正参数β
    public static final double B = DynamicConf.beta();
    private double transferTime = TRANSFER_TIME;
    private double stopStationTime = STOP_STATION_TIME;
    private double transferPenalty = TRANSFER_PENALTY;
    private double sectionWeightAddRate = SECTION_WEIGHT_ADD_RATE;
    private double a = 1.7;
    private double b = 4;

    public double getTransferTime() {
        return transferTime;
    }

    public double getStopStationTime() {
        return stopStationTime;
    }

    public double getTransferPenalty() {
        return transferPenalty;
    }

    public double getSectionWeightAddRate() {
        return sectionWeightAddRate;
    }

    public double getA() {
        return a;
    }

    public double getB() {
        return b;
    }

    public void setTransferTime(double transferTime) {
        this.transferTime = transferTime;
    }

    public void setStopStationTime(double stopStationTime) {
        this.stopStationTime = stopStationTime;
    }

    public void setTransferPenalty(double transferPenalty) {
        this.transferPenalty = transferPenalty;
    }

    public void setSectionWeightAddRate(double sectionWeightAddRate) {
        this.sectionWeightAddRate = sectionWeightAddRate;
    }

    public void setA(double a) {
        this.a = a;
    }

    public void setB(double b) {
        this.b = b;
    }
}
