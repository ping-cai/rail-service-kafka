package flowreduce;

import kspcalculation.Edge;

import java.io.Serializable;
import java.util.Objects;

public class SimpleTransfer  implements Serializable {
    private String inId;
    private String outId;
    private String transferDirection;

    public SimpleTransfer(String inId, String outId, String transferDirection) {
        this.inId = inId;
        this.outId = outId;
        this.transferDirection = transferDirection;
    }

    public SimpleTransfer(Edge edge, String beforeDirection, String afterDirection) {
        this.inId = edge.getFromNode();
        this.outId = edge.getFromNode();
        this.transferDirection = String.format("%s%s", beforeDirection, afterDirection);
    }

    public String getInId() {
        return inId;
    }

    public String getOutId() {
        return outId;
    }

    public String getTransferDirection() {
        return transferDirection;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleTransfer that = (SimpleTransfer) o;
        return Objects.equals(inId, that.inId) &&
                Objects.equals(outId, that.outId) &&
                Objects.equals(transferDirection, that.transferDirection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inId, outId, transferDirection);
    }
}
