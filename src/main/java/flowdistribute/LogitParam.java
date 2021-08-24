package flowdistribute;

import java.io.Serializable;

public class LogitParam implements Serializable {
    private static Double distributionParam;

    public static Double getDistributionParam(Integer kNum) {
        double param = 3.0 + kNum * Math.random() * 0.5;
        if (param > 3.5) {
            return 3.5;
        } else {
            return param;
        }
    }
}
