package flowreduce;

import java.io.Serializable;
import java.util.Objects;

public class SimpleSection implements Serializable {
    private Integer sectionId;
    private String inId;
    private String outId;

    public SimpleSection(Integer sectionId, String inId, String outId) {
        this.sectionId = sectionId;
        this.inId = inId;
        this.outId = outId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleSection that = (SimpleSection) o;
        return Objects.equals(inId, that.inId) &&
                Objects.equals(outId, that.outId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inId, outId);
    }

    public Integer getSectionId() {
        return sectionId;
    }

    public String getInId() {
        return inId;
    }

    public String getOutId() {
        return outId;
    }

    @Override
    public String toString() {
        return "简单区间结果{" +
                "区间ID=" + sectionId +
                ", 起始id='" + inId + '\'' +
                ", 终止id='" + outId + '\'' +
                '}';
    }
}
