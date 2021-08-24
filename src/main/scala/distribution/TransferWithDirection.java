package distribution;

import domain.Section;

import java.io.Serializable;
import java.util.Objects;

public class TransferWithDirection implements Serializable {
    private Section section;
    /*
        11是上进上出
        22是下进下出
         */

    private String direction;

    public TransferWithDirection(Section section, String direction) {
        this.section = section;
        this.direction = direction;
    }

    public Section getSection() {
        return section;
    }

    public String getDirection() {
        return direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransferWithDirection that = (TransferWithDirection) o;
        return Objects.equals(section, that.section) &&
                Objects.equals(direction, that.direction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(section, direction);
    }

    @Override
    public String toString() {
        return "TransferWithDirection{" +
                "section=" + section +
                ", direction='" + direction + '\'' +
                '}';
    }
}
