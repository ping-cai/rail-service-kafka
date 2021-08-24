package kspcalculation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SectionStrData extends AbstractSection {
    private String fromId;
    private String toId;
    private String direction;

    public SectionStrData(String fromId, String toId) {
        this.fromId = fromId;
        this.toId = toId;
    }

    public SectionStrData(Edge edge) {
        this.fromId = edge.getFromNode();
        this.toId = edge.getToNode();
        super.weight = edge.getWeight();
    }

    public SectionStrData(Integer sectionId, double weight, String fromId, String toId, String direction) {
        super(sectionId, weight);
        this.fromId = fromId;
        this.toId = toId;
        this.direction = direction;
    }

    public SectionStrData(String fromId, String toId, String direction) {
        this.fromId = fromId;
        this.toId = toId;
        this.direction = direction;
    }

    public String getFromId() {
        return fromId;
    }

    public String getToId() {
        return toId;
    }

    public String getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SectionStrData that = (SectionStrData) o;
        return Objects.equals(fromId, that.fromId) &&
                Objects.equals(toId, that.toId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromId, toId);
    }

    public static List<Edge> getEdgesOfId(List<SectionStrData> sectionDataList) {
        List<Edge> edges = new ArrayList<>();
        for (SectionStrData section : sectionDataList) {
            Edge edge = new Edge();
            String fromId = section.getFromId();
            String toId = section.getToId();
            edge.setFromNode(fromId);
            edge.setToNode(toId);
            edge.setWeight(section.getWeight());
            edges.add(edge);
        }
        return edges;
    }
}
