package domain;

import java.io.Serializable;

public class SectionTravelSecondWithRate implements Serializable {
    private Section section;
    private Integer timeSeconds;
    private Double rate;

    public SectionTravelSecondWithRate(Section section, Integer timeSeconds, Double rate) {
        this.section = section;
        this.timeSeconds = timeSeconds;
        this.rate = rate;
    }

    public Section getSection() {
        return section;
    }

    public Integer getTimeSeconds() {
        return timeSeconds;
    }

    public Double getRate() {
        return rate;
    }

}
