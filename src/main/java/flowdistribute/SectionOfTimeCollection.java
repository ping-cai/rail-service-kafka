package flowdistribute;

import kspcalculation.DirectedEdge;

import java.io.Serializable;

public class SectionOfTimeCollection implements Serializable {
    private String startTime;
    private String endTime;
    private DirectedEdge directedEdge;

    public SectionOfTimeCollection(String startTime, String endTime, DirectedEdge directedEdge) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.directedEdge = directedEdge;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public DirectedEdge getDirectedEdge() {
        return directedEdge;
    }
}
