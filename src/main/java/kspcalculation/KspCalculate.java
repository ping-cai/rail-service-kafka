package kspcalculation;

import flowdistribute.OdData;

import java.util.List;

public interface KspCalculate {
    List<DirectedPath> computeKsp(OdData odData);
}
