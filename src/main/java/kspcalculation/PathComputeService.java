package kspcalculation;

import domain.Section;
import exception.PathNotHaveException;
import flowdistribute.OdData;

import java.io.Serializable;
import java.util.*;

public class PathComputeService implements Serializable {
    private int kspNum = 10;
    private Graph graph;
    private Map<Section, String> sectionWithDirectionMap;

    public PathComputeService(int kspNum, Graph graph, Map<Section, String> sectionWithDirectionMap) {
        this.kspNum = kspNum;
        this.graph = graph;
        this.sectionWithDirectionMap = sectionWithDirectionMap;
    }

    public PathComputeService(int kspNum) {
        this.kspNum = kspNum;
    }

    public PathComputeService(Graph graph) {
        this.graph = graph;
    }

    public PathComputeService(int kspNum, Graph graph) {
        this.kspNum = kspNum;
        this.graph = graph;
    }

    /**
     * @param source 进站，源地址
     * @param target 出站，目的地址
     * @param k      K短路参数
     * @return K短路List<Path> 集合
     */
    public static List<Path> computeODPath(Graph graph, String source, String target, int k) {
        Yen yenAlgorithm = new Yen();
        return yenAlgorithm.ksp(graph, source, target, k);
    }

    public List<Path> fixedPathOdCompute(Graph graph, OdData odWithTime) throws PathNotHaveException {
        Yen yenAlgorithm = new Yen();
        List<Path> ksp = yenAlgorithm.ksp(graph, odWithTime.getInId(), odWithTime.getOutId(), kspNum);
        if (ksp.size() < 1) {
            throw new PathNotHaveException(String.format("the K path s，Please check whether the road network is correct and The OD data is %s,%s", odWithTime.getInId(), odWithTime.getOutId()));
        }
        return ksp;
    }

    public static List<Path> dynamicPathOdCompute(Graph graph, OdData odWithTime, SectionAssociationMap sectionAssociationMap) throws PathNotHaveException {
        Yen yenAlgorithm = new Yen();
        List<Path> onePath = yenAlgorithm.ksp(graph, odWithTime.getInId(), odWithTime.getOutId(), 1);
        if (onePath.isEmpty()) {
            throw new PathNotHaveException(" The K path search result is empty. Please check whether the road network is correct！", odWithTime);
        }
        Path path = onePath.get(0);
        if (path == null) {
            throw new PathNotHaveException(" The K path search result is empty. Please check whether the road network is correct！", odWithTime);
        }
        LinkedList<Edge> edges = path.getEdges();
        int transferNum = 0;
        for (Edge edge : edges) {
            SectionAllInfo sectionInfo = sectionAssociationMap.getSectionInfo(edge);
            String direction = sectionInfo.getDirection();
            if ("0".equals(direction)) {
                transferNum++;
            }
        }
        return yenAlgorithm.ksp(graph, odWithTime.getInId(), odWithTime.getOutId(), transferNum + 1);
    }

    public List<Path> fixedKPathCompute(Graph graph, String source, String target) {
        Yen yenAlgorithm = new Yen();
        return yenAlgorithm.ksp(graph, source, target, kspNum);
    }

    public static Map<Path, Integer> removeTheFirstAndLastTransfer(SectionAssociationMap sectionAssociationMap,
                                                                   List<Path> kspResult) {
        Map<Path, Integer> pathMap = new HashMap<>();
        for (Path path : kspResult) {
            if (path == null) {
                throw new PathNotHaveException("Empty when traversing the searched path! Please check the reason!");
            }
            double totalCost = path.getTotalCost();
            LinkedList<Edge> edges = path.getEdges();
            removeEdges(sectionAssociationMap, edges);
            int sumTransfer = 0;
            for (Edge edge : edges) {
                if ("0".equals(sectionAssociationMap.getSectionInfo(edge).getDirection())) {
                    sumTransfer++;
                }
            }
            Path newPath = new Path();
            newPath.setEdges(edges);
            newPath.setTotalCost(totalCost);
            pathMap.put(newPath, sumTransfer);
        }
        return pathMap;
    }

    private static void removeEdges(SectionAssociationMap sectionAssociationMap, LinkedList<Edge> edges) {
        while (edges.size() > 0 &&
                ("0".equals(sectionAssociationMap.getSectionInfo(edges.getFirst()).getDirection())
                        || "0".equals(sectionAssociationMap.getSectionInfo(edges.getLast()).getDirection()))) {
            if ("0".equals(sectionAssociationMap.getSectionInfo(edges.getFirst()).getDirection())) {
                edges.removeFirst();
            }
            if (edges.size() > 0 && "0".equals(sectionAssociationMap.getSectionInfo(edges.getLast()).getDirection())) {
                edges.removeLast();
            }
        }
    }

    public int getKspNum() {
        return kspNum;
    }

    public Graph getGraph() {
        return graph;
    }

    public Map<Section, String> getSectionWithDirectionMap() {
        return sectionWithDirectionMap;
    }
}
