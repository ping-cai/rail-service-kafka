package flowdistribute;

import java.io.Serializable;
import java.util.Set;

public class LineWithFlow implements Serializable {
    private Set<String> lines;
    private Double passengers;

    public LineWithFlow(Set<String> lines, Double passengers) {
        this.lines = lines;
        this.passengers = passengers;
    }

    public Set<String> getLines() {
        return lines;
    }

    public Double getPassengers() {
        return passengers;
    }

    @Override
    public String toString() {
        return "LineWithFlow{" +
                "lines=" + lines +
                ", passengers=" + passengers +
                '}';
    }
}
