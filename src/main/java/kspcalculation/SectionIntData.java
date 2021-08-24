package kspcalculation;

import java.util.Objects;

public class SectionIntData extends AbstractSection {
    private Integer fromId;
    private Integer toId;
    private String direction;

    public SectionIntData(Integer fromId, Integer toId) {
        this.fromId = fromId;
        this.toId = toId;
    }

    public SectionIntData(Integer sectionId, double weight, Integer fromId, Integer toId, String direction) {
        super(sectionId, weight);
        this.fromId = fromId;
        this.toId = toId;
        this.direction = direction;
    }

    public Integer getFromId() {
        return fromId;
    }

    public Integer getToId() {
        return toId;
    }

    public String getDirection() {
        return direction;
    }

    public void setFromId(Integer fromId) {
        this.fromId = fromId;
    }

    public void setToId(Integer toId) {
        this.toId = toId;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getSection() {
        return String.format("%s %s", fromId, toId);
    }

    @Override
    public String toString() {
        return "SectionIntData{" +
                "fromId=" + fromId +
                ", toId=" + toId +
                ", direction='" + direction + '\'' +
                ", sectionId=" + sectionId +
                ", weight=" + weight +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SectionIntData that = (SectionIntData) o;
        return Objects.equals(fromId, that.fromId) &&
                Objects.equals(toId, that.toId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromId, toId);
    }
}
