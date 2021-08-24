package distribution;

import java.io.Serializable;
import java.util.Objects;

public class StationWithType implements Serializable {
    private String stationId;
    private String type;

    public StationWithType(String stationId, String type) {
        this.stationId = stationId;
        this.type = type;
    }

    public String getStationId() {
        return stationId;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StationWithType that = (StationWithType) o;
        return Objects.equals(stationId, that.stationId) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, type);
    }

    @Override
    public String toString() {
        return "StationWithType{" +
                "stationId='" + stationId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
