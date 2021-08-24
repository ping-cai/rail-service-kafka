package flowdistribute;

import kspcalculation.DirectedPath;

import java.util.Map;
import java.util.Set;

public interface KspDistribution {
    public Map<Set<Line>, Double> lineDistribution(OdWithTime odWithTime, Map<DirectedPath, Double> logitResult);
}
