package kspcalculation;

import flowdistribute.OdData;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KspServiceImpl implements KspCalculate {
    private Map<String, SectionIntData> sectionOnlyIdMap;
    private KSPUtil kspUtil;

    public KspServiceImpl(KSPUtil kspUtil) {
        this.kspUtil = kspUtil;
    }

    public KspServiceImpl(Map<String, SectionIntData> sectionOnlyIdMap, KSPUtil kspUtil) {
        this.sectionOnlyIdMap = sectionOnlyIdMap;
        this.kspUtil = kspUtil;
    }

    public Map<String, SectionIntData> getSectionOnlyIdMap() {
        return sectionOnlyIdMap;
    }

    public static List<Edge> getEdgesOfId(List<SectionIntData> sectionIntDataList) {
        List<Edge> edges = new ArrayList<Edge>();
        for (SectionIntData section : sectionIntDataList) {
            Edge edge = new Edge();
            String fromId = section.getFromId().toString();
            String toId = section.getToId().toString();
            edge.setFromNode(fromId);
            edge.setToNode(toId);
            edge.setWeight(section.getWeight());
            edges.add(edge);
        }
        return edges;
    }

    public List<DirectedPath> getDirectedPath(List<Path> computeKspResult) {
        List<DirectedPath> directedPaths = new ArrayList<DirectedPath>();
        for (int i = 0; i < computeKspResult.size(); i++) {
            Path path = computeKspResult.get(i);
            LinkedList<Edge> edges = path.getEdges();
            List<DirectedEdge> directedEdgeList = new LinkedList<DirectedEdge>();
            for (int j = 0; j < edges.size(); j++) {
                DirectedEdge directedEdge = new DirectedEdge();
                String fromNode = edges.get(j).getFromNode();
                String toNode = edges.get(j).getToNode();
                directedEdge.setEdge(edges.get(j));
                directedEdge.setDirection(sectionOnlyIdMap.get(String.format("%s %s", fromNode, toNode)).getDirection());
                directedEdgeList.add(directedEdge);
            }
            DirectedPath directedPath = new DirectedPath(directedEdgeList);
            directedPaths.add(directedPath);
        }
        return directedPaths;
    }

    public List<Path> computeKsp(String inId, String outId) {
        List<Path> odPath = kspUtil.computeODPath(inId, outId, 1);
        int paramK = getChangeParamK(odPath);
        return kspUtil.computeODPath(inId, outId, paramK);
    }

    private int getChangeParamK(List<Path> odPath) {
        Map<String, String> transferNodes = kspUtil.getTransferNodes();
        Path path = odPath.get(0);
        LinkedList<Edge> edges = path.getEdges();
        int kCount = 1;
        for (int i = 0; i < edges.size() - 1; i++) {
            String fromNode = edges.get(i).getFromNode();
            String toNode = edges.get(i).getToNode();
            String judgeTransfer = String.format("%s %s", fromNode, toNode);
            if (transferNodes.containsKey(judgeTransfer)) {
                kCount++;
            }
        }
        return kCount;
    }

    public List<DirectedPath> computeKsp(OdData odData) {
        List<Path> odPath = kspUtil.computeODPath(odData.getInId(), odData.getOutId(), 1);
        int paramK = getChangeParamK(odPath);
        return getDirectedPath(kspUtil.computeODPath(odData.getInId(), odData.getOutId(), paramK));
    }
}
