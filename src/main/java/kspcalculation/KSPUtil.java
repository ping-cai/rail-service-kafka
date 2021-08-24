package kspcalculation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class KSPUtil implements Serializable {
    private Graph graph;//路网图
    private List<Edge> edges;//构建路网图的边
    private Map<String, String> transferNodes;//换乘路网节点

    public KSPUtil(Graph graph) {
        this.graph = graph;
        this.edges = graph.getEdgeList();
    }

    public KSPUtil(Graph graph, Map<String, String> transferNodes) {
        this.graph = graph;
        this.edges = graph.getEdgeList();
        this.transferNodes = transferNodes;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public Map<String, String> getTransferNodes() {
        return transferNodes;
    }

    /**
     * @param source 进站，源地址
     * @param target 出站，目的地址
     * @param k      K短路参数
     * @return K短路List<Path> 集合
     */
    public List<Path> computeODPath(String source, String target, int k) {
        Yen yenAlgorithm = new Yen();
        List<Path> ksp = yenAlgorithm.ksp(graph, source, target, k);
        return ksp;
    }

}
