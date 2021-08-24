package costcompute;

import java.io.Serializable;

public class Train implements Serializable {
    private String lineName;
    private Integer seats;
    private Integer maxCapacity;

    public Train(String lineName, Integer seats, Integer maxCapacity) {
        this.lineName = lineName;
        this.seats = seats;
        this.maxCapacity = maxCapacity;
    }

    public String getLineName() {
        return lineName;
    }

    public Integer getSeats() {
        return seats;
    }

    public Integer getMaxCapacity() {
        return maxCapacity;
    }
}
