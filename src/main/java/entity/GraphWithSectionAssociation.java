package entity;

import domain.Section;
import domain.TravelTimeAndRate;
import kspcalculation.Graph;
import kspcalculation.SectionAssociationMap;

import java.io.Serializable;
import java.util.Map;

public class GraphWithSectionAssociation implements Serializable {
    private Graph graph;
    private SectionAssociationMap associationMap;
    private Map<Section, TravelTimeAndRate> sectionTravelTimeAndRateMap;
    public GraphWithSectionAssociation(Graph graph, SectionAssociationMap associationMap) {
        this.graph = graph;
        this.associationMap = associationMap;
    }

    public GraphWithSectionAssociation(Graph graph, SectionAssociationMap associationMap, Map<Section, TravelTimeAndRate> sectionTravelTimeAndRateMap) {
        this.graph = graph;
        this.associationMap = associationMap;
        this.sectionTravelTimeAndRateMap = sectionTravelTimeAndRateMap;
    }

    public Map<Section, TravelTimeAndRate> getSectionTravelTimeAndRateMap() {
        return sectionTravelTimeAndRateMap;
    }

    public Graph getGraph() {
        return graph;
    }

    public SectionAssociationMap getAssociationMap() {
        return associationMap;
    }
}
