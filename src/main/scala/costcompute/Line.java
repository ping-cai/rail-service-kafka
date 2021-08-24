package costcompute;

import domain.Section;

import java.util.Set;

public class Line {
    private Set<Section> sectionList;
    private Train train;

    public Line(Set<Section> sectionList, Train train) {
        this.sectionList = sectionList;
        this.train = train;
    }

    public Set<Section> getSectionList() {
        return sectionList;
    }

    public Train getTrain() {
        return train;
    }
}
