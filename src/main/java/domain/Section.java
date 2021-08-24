package domain;

import kspcalculation.Edge;

import java.io.Serializable;
import java.util.Objects;

public class Section implements Serializable {
    private String inId;
    private String outId;

    public String getInId() {
        return inId;
    }

    public String getOutId() {
        return outId;
    }

    public Section(String inId, String outId) {
        this.inId = inId;
        this.outId = outId;
    }

    public static Section createSectionByEdge(Edge edge) {
        return new Section(edge.getFromNode(), edge.getToNode());
    }

    public void setInId(String inId) {
        this.inId = inId;
    }

    public void setOutId(String outId) {
        this.outId = outId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Section section = (Section) o;
        return Objects.equals(inId, section.inId) &&
                Objects.equals(outId, section.outId);
    }

    @Override
    public String toString() {
        return "Section{" +
                "inId='" + inId + '\'' +
                ", outId='" + outId + '\'' +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(inId, outId);
    }
}
