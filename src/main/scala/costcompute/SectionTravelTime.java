package costcompute;

import domain.Section;

import java.io.Serializable;

public class SectionTravelTime implements Serializable {
    private Section section;
    private TravelTime travelTime;

    public SectionTravelTime(Section section, TravelTime travelTime) {
        this.section = section;
        this.travelTime = travelTime;
    }

    public Section getSection() {
        return section;
    }

    public TravelTime getTravelTime() {
        return travelTime;
    }

    @Override
    public String toString() {
        return "SectionTravelTime{" +
                "section=" + section +
                ", travelTime=" + travelTime +
                '}';
    }
}
