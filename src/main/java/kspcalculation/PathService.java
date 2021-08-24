package kspcalculation;

import conf.DynamicConf;
import domain.Section;
import exception.PathNotHaveException;

import java.io.Serializable;
import java.util.*;

public class PathService implements Serializable {
    private int kspNum = 10;
    private Graph graph;
    private Map<Section, String> sectionWithDirectionMap;

    public PathService(int kspNum, Graph graph, Map<Section, String> sectionWithDirectionMap) {
        this.kspNum = kspNum;
        this.graph = graph;
        this.sectionWithDirectionMap = sectionWithDirectionMap;
    }

    public List<Path> getLegalKPath(String source, String target) {
        Yen yenAlgorithm = new Yen();
        List<Path> pathList = yenAlgorithm.ksp(graph, source, target, kspNum);
        return removeRepeated(pathList);
    }

    //需要移除首尾换乘的区间以及相邻换乘的区间，并且不经过重复的站点
    private List<Path> removeRepeated(List<Path> kspResult) {
        LinkedList<Path> removedPath = new LinkedList<>();
        if (kspResult.isEmpty())
            throw new PathNotHaveException("kspResult is  Empty when traversing the searched path! Please check the reason!");
        for (Path path : kspResult) {
            double totalCost = path.getTotalCost();
            LinkedList<Edge> edges = path.getEdges();
            if (edges.isEmpty()) {
                continue;
            }
            LinkedList<Edge> newEdges = new LinkedList<>();
            //            去除重复经过的站的路径
            HashSet<Edge> edgeSet = new HashSet<>();
//            是否经过同一站点
            boolean repeatOneStation = false;
//            此处的长度会变短
//          是否经过两次连续换乘
            boolean continuousTransfer = false;
            for (int i = 0; i < edges.size(); i++) {
                Edge nextEdge = edges.get(i);
                if (edgeSet.contains(nextEdge)) {
                    repeatOneStation = true;
                    break;
                } else {
                    edgeSet.add(nextEdge);
                }
//                需要把第一个不是换乘的边集放入newEdges中，确保第一个站不是换乘
                Section everySection = Section.createSectionByEdge(nextEdge);
                if (newEdges.isEmpty() && sectionWithDirectionMap.get(everySection) != null) {
                    newEdges.add(nextEdge);
//                    当newEdges不为空的时候开始进行一下遍历
                } else if (!newEdges.isEmpty()) {
//                    如果连续两次换乘，直接跳出，下一条路径
                    if (i < edges.size() - 1) {
                        Edge doubleNextEdge = edges.get(i + 1);
                        Section doubleNextSecond = Section.createSectionByEdge(doubleNextEdge);
                        if (null == sectionWithDirectionMap.get(everySection) && null == sectionWithDirectionMap.get(doubleNextSecond)) {
                            continuousTransfer = true;
                            break;
                        }
                    }
                    Section lastSection = Section.createSectionByEdge(newEdges.getLast());
                    if (sectionWithDirectionMap.get(lastSection) == null && sectionWithDirectionMap.get(everySection) == null) {
                        totalCost -= DynamicConf.transferPenalty();
                    } else {
                        newEdges.add(nextEdge);
                    }
                }
            }

//            如果经过同一个站，那么就直接下一个路径
            if (repeatOneStation) {
                continue;
            }
//            为空则跳过当前路径
            if (newEdges.isEmpty()) {
                continue;
            }
            if (continuousTransfer) {
                continue;
            }
            if (null == sectionWithDirectionMap.get(Section.createSectionByEdge(newEdges.getLast()))) {
                newEdges.removeLast();
                totalCost -= DynamicConf.transferPenalty();
            }
            path.setEdges(newEdges);
            path.setTotalCost(totalCost);
            removedPath.add(path);
        }
        if (removedPath.isEmpty())
            throw new PathNotHaveException(" removedPath is Empty when traversing the searched path! Please check the reason!");
        return removedPath;
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
