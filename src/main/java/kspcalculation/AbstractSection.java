package kspcalculation;

import java.io.Serializable;

public abstract class AbstractSection implements Serializable {
    protected Integer sectionId;
    protected double weight;

    public AbstractSection(Integer sectionId, double weight) {
        this.sectionId = sectionId;
        this.weight = weight;
    }

    public AbstractSection() {
    }

    public Integer getSectionId() {
        return sectionId;
    }

    public double getWeight() {
        return weight;
    }
}
