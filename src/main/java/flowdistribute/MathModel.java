package flowdistribute;

import kspcalculation.DirectedPath;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MathModel implements Serializable {
    public static Map<DirectedPath, Double> logit(List<DirectedPath> directedPathList, Double passengers) {
        Map<DirectedPath, Double> logitResult = new HashMap<DirectedPath, Double>();
        double exp = Math.E;
        double denominator = 0;
        int pathSize = directedPathList.size();
        Double distributionParam = LogitParam.getDistributionParam(pathSize);
        for (DirectedPath directedPath : directedPathList) {
            denominator = denominator + Math.pow(exp, -distributionParam * directedPath.getTotalCost());
        }

        for (int i = 0; i < pathSize; i++) {
            DirectedPath directedPath = directedPathList.get(i);
            double molecule = Math.pow(exp, -distributionParam * directedPath.getTotalCost());
            logitResult.put(directedPath, passengers * (molecule / denominator));
        }
        return logitResult;
    }

    public static double euclideanDistance(double x, double y) {
        return Math.sqrt(Math.pow(x - y, 2));
    }
}
