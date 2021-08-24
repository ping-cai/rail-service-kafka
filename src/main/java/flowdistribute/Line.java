package flowdistribute;

import kspcalculation.DirectedEdge;
import kspcalculation.DirectedPath;

import java.io.Serializable;
import java.util.*;

public class Line implements Serializable {
    private Map<String, String> lineMap;

    public Line(Map<String, String> lineMap) {
        this.lineMap = lineMap;
    }

    public Map<String, String> getLineMap() {
        return lineMap;
    }

    public String getLineName(OdData odData) {
        return lineMap.get(String.format("%s %s", odData.getInId(), odData.getOutId()));
    }

    public Set<String> getLines(DirectedPath directedPath) {
        List<DirectedEdge> edges = directedPath.getEdges();
        Set<String> lines = new HashSet<String>();
        for (DirectedEdge edge : edges) {
            String fromNode = edge.getEdge().getFromNode();
            String toNode = edge.getEdge().getToNode();
            String lineName = getLineName(new OdWithTime(fromNode, toNode));
            if (null != lineName) {
                lines.add(lineName);
            }
        }
        return lines;
    }

    public List<LineWithFlow> linesPassengers(Map<DirectedPath, Double> directedPathDoubleMap) {
        List<LineWithFlow> lineWithFlowArrayList = new ArrayList<LineWithFlow>();
        for (Map.Entry<DirectedPath, Double> entry : directedPathDoubleMap.entrySet()) {
            Set<String> lines = getLines(entry.getKey());
            Double passengers = entry.getValue();
            lineWithFlowArrayList.add(new LineWithFlow(lines, passengers));
        }
        return lineWithFlowArrayList;
    }
}
