package domain;

import java.io.Serializable;
import java.util.Objects;

public class LinePassengers implements Serializable {
    private String lineName;
    private Double flow;

    public LinePassengers(String lineName, Double flow) {
        this.lineName = lineName;
        this.flow = flow;
    }

    public String getLineName() {
        return lineName;
    }

    public Double getFlow() {
        return flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinePassengers that = (LinePassengers) o;
        return Objects.equals(lineName, that.lineName) &&
                Objects.equals(flow, that.flow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lineName, flow);
    }

    @Override
    public String toString() {
        return "LinePassengers{" +
                "lineName='" + lineName + '\'' +
                ", flow=" + flow +
                '}';
    }
}
