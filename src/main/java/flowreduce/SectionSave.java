package flowreduce;

import utils.TimeKey;

import java.io.Serializable;

public class SectionSave extends TimeKey implements Serializable {
    private Integer sectionId;
    private String inId;
    private String outId;
    private float flow;

    public SectionSave(String startTime, String endTime) {
        super(startTime, endTime);
    }

    public SectionSave(String startTime, String endTime, Integer sectionId, String inId, String outId, float flow) {
        super(startTime, endTime);
        this.sectionId = sectionId;
        this.inId = inId;
        this.outId = outId;
        this.flow = flow;
    }

}
