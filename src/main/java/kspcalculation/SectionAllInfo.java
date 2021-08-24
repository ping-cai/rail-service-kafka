package kspcalculation;

import java.io.Serializable;

public class SectionAllInfo extends AbstractSection implements Serializable {
    private String lineName;
    private String direction;


    public SectionAllInfo(Integer sectionId, double weight, String lineName, String direction) {
        super(sectionId, weight);
        this.lineName = lineName;
        this.direction = direction;
    }

    public SectionAllInfo(String lineName, String direction) {
        this.lineName = lineName;
        this.direction = direction;
    }

    public SectionAllInfo(Integer sectionId, double weight, String direction) {
        super(sectionId, weight);
        this.direction = direction;
    }

    public SectionAllInfo(String lineName) {
        this.lineName = lineName;
    }

    public String getLineName() {
        return lineName;
    }

    public String getDirection() {
        return direction;
    }
}
