package domain;

import java.io.Serializable;

public class StationInfo implements Serializable {
    private String stationId;
    private String type;
    private String stationName;
    private String line;

    public StationInfo(String stationId, String type, String stationName, String line) {
        this.stationId = stationId;
        this.type = type;
        this.stationName = stationName;
        this.line = line;
    }

    public String getStationId() {
        return stationId;
    }

    public String getType() {
        return type;
    }

    public String getStationName() {
        return stationName;
    }

    public String getLine() {
        return line;
    }

    @Override
    public String toString() {
        return "StationInfo{" +
                "stationId='" + stationId + '\'' +
                ", type='" + type + '\'' +
                ", stationName='" + stationName + '\'' +
                ", line='" + line + '\'' +
                '}';
    }
}
