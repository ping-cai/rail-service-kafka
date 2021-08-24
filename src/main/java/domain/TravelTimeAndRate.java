package domain;

import java.io.Serializable;

public class TravelTimeAndRate implements Serializable {
    private Integer seconds;
    private Double rate;

    public TravelTimeAndRate(Integer seconds, Double rate) {
        this.seconds = seconds;
        this.rate = rate;
    }

    public Integer getSeconds() {
        return seconds;
    }

    public Double getRate() {
        return rate;
    }
}
