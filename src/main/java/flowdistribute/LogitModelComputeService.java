package flowdistribute;

import exception.PathNotHaveException;
import kspcalculation.*;

import java.io.Serializable;
import java.util.*;

public class LogitModelComputeService implements Serializable {
    public static final double THETA = 3.5;
    public static final double ALPHA = 0.0;
    public static final double BETA = 0.0;
    public static final double LENGTH_CONVERSION_RATE = 1.5;
    public static final double TRANSFER_PENALTIES = 24.0;
    public static final double STOP_STATION_TIME = 1.0;
    public static final double ZERO_PARAMETER = 0.1;
    private String distributionType = "dynamic";

    public LogitModelComputeService() {
    }

    public LogitModelComputeService(String distributionType) {
        this.distributionType = distributionType;
    }

    private static Double getDistributionParam(Integer kNum) {
        double param = THETA + kNum * Math.random() * 0.5;
        if (param > 3.5) {
            return 3.5;
        } else {
            return param;
        }
    }

    public static SimpleLogitResult logit(KPathResult kPathResult, LogitModelComputeService logitModelComputeService) {
        Double passengers = kPathResult.getOdWithTime().getPassengers();
        Map<Path, Integer> pathWithTransferNum = kPathResult.getPathWithTransferNum();
        double exp = Math.E;
        double denominator = 0.0;
        int pathSize = pathWithTransferNum.size();
        if (pathSize < 1) {
            throw new PathNotHaveException("K短路计算得到的集合为空！请检查原因!");
        }
        Double distributionParam = getDistributionParam(pathSize);
        List<Double> minCostList = new ArrayList<>();
        for (Map.Entry<Path, Integer> pathMap : pathWithTransferNum.entrySet()) {
            Path path = pathMap.getKey();
            Integer transferTimes = pathMap.getValue();
            int edgeNum = path.getEdges().size();
            double generalizedCostChange = logitModelComputeService.generalizedCostChange(path.getTotalCost(), transferTimes, edgeNum);
            minCostList.add(logitModelComputeService.minCostChange(path.getTotalCost(), transferTimes, edgeNum));
            path.setTotalCost(generalizedCostChange);
        }
        Double minCost = Collections.min(minCostList);
        for (Map.Entry<Path, Integer> pathMap : pathWithTransferNum.entrySet()) {
            Path path = pathMap.getKey();
            double totalCost = path.getTotalCost();
            denominator = denominator + Math.pow(exp, -distributionParam * (totalCost / minCost));
        }
        Map<Path, Double> pathFlow = new HashMap<>();
        for (Map.Entry<Path, Integer> pathMap : pathWithTransferNum.entrySet()) {
            Path path = pathMap.getKey();
            double totalCost = path.getTotalCost();
            double molecule = Math.pow(exp, -distributionParam * (totalCost / minCost));
            double rate = molecule / denominator;
            double flow = rate * passengers;
            if (Double.isNaN(flow)) {
                flow = 0.0;
            }
            pathFlow.put(path, flow);
        }
        return new SimpleLogitResult(kPathResult.getOdWithTime(), pathFlow);
    }

    private static double generalizedCostSum(double pathWeight, double transferTimes, int pathEdges) {
        return pathWeight * LENGTH_CONVERSION_RATE + minGeneralizedCost(transferTimes, pathEdges);
    }

    private static double minGeneralizedCost(double transferTimes, int pathEdges) {
        return transferTimes * TRANSFER_PENALTIES + pathEdges * STOP_STATION_TIME;
    }

    private double generalizedCostChange(double pathWeight, double transferTimes, int pathEdges) {
        if ("dynamic".equals(distributionType)) {
            return generalizedCostSum(pathWeight, transferTimes, pathEdges);
        }
        return pathWeight;
    }

    private double minCostChange(double pathWeight, double transferTimes, int pathEdges) {
        if ("dynamic".equals(distributionType)) {
            return minGeneralizedCost(transferTimes, pathEdges);
        }
        return pathWeight;
    }

}
