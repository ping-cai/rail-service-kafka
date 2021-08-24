package flowreduce;

import java.io.Serializable;
import java.util.Objects;

public class SimpleStation implements Serializable {
    private String stationId;
    private String typeOfInOrOut;

    public SimpleStation(String stationId, String typeOfInOrOut) {
        this.stationId = stationId;
        this.typeOfInOrOut = typeOfInOrOut;
    }

    public String getStationId() {
        return stationId;
    }

    public String getTypeOfInOrOut() {
        return typeOfInOrOut;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleStation that = (SimpleStation) o;
        return Objects.equals(stationId, that.stationId) &&
                Objects.equals(typeOfInOrOut, that.typeOfInOrOut);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stationId, typeOfInOrOut);
    }
}
