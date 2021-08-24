package kspcalculation;

import domain.SectionInfo;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * The Edge class implements standard properties and methods for A weighted edge in A directed graph.
 * <p>
 * Created by Brandon Smock on 6/19/15.
 */
public class Edge implements Cloneable, Serializable {
    private String fromNode;
    private String toNode;
    private double weight;

    public Edge() {
        this.fromNode = null;
        this.toNode = null;
        this.weight = Double.MAX_VALUE;
    }

    public Edge(String fromNode, String toNode, double weight) {
        this.fromNode = fromNode;
        this.toNode = toNode;
        this.weight = weight;
    }

    public static List<Edge> getEdgeBySections(List<SectionInfo> sectionInfoList) {
        List<Edge> edges = new LinkedList<>();
        for (SectionInfo sectionInfo : sectionInfoList) {
            Edge edge = new Edge(sectionInfo.getSection().getInId(), sectionInfo.getSection().getOutId(), sectionInfo.getWeight());
            edges.add(edge);
        }
        return edges;
    }

    public static List<Edge> appendEdgeList(List<SectionInfo> sectionInfoList, List<Edge> edgeLinkedList) {
        List<Edge> edges = new LinkedList<>();
        for (SectionInfo sectionInfo : sectionInfoList) {
            Edge edge = new Edge(sectionInfo.getSection().getInId(), sectionInfo.getSection().getOutId(), sectionInfo.getWeight());
            edges.add(edge);
        }
        edgeLinkedList.addAll(edges);
        return edgeLinkedList;
    }

    public String getFromNode() {
        return fromNode;
    }

    public void setFromNode(String fromNode) {
        this.fromNode = fromNode;
    }

    public String getToNode() {
        return toNode;
    }

    public void setToNode(String toNode) {
        this.toNode = toNode;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public Edge clone() {
        return new Edge(fromNode, toNode, weight);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,%s)", fromNode, toNode, weight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return Double.compare(edge.weight, weight) == 0 &&
                Objects.equals(fromNode, edge.fromNode) &&
                Objects.equals(toNode, edge.toNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromNode, toNode, weight);
    }
}