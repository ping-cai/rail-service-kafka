package domain;


import java.io.Serializable;

public class SectionInfo implements Serializable {
    private Integer sectionId;
    private Section section;
    private String direction;
    private Double weight;
    private String line;

    public SectionInfo(Integer sectionId, Section section, String direction, Double weight, String line) {
        this.sectionId = sectionId;
        this.section = section;
        this.direction = direction;
        this.weight = weight;
        this.line = line;
    }

    public Integer getSectionId() {
        return sectionId;
    }

    public Section getSection() {
        return section;
    }

    public String getDirection() {
        return direction;
    }

    public Double getWeight() {
        return weight;
    }

    public String getLine() {
        return line;
    }

    @Override
    public String toString() {
        return "SectionInfo{" +
                "sectionId=" + sectionId +
                ", section=" + section +
                ", direction='" + direction + '\'' +
                ", weight=" + weight +
                ", line='" + line + '\'' +
                '}';
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }
}
