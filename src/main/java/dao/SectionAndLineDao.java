package dao;

import flowreduce.LineSave;
import flowreduce.SectionSave;

import java.util.List;

public class SectionAndLineDao {
    private List<SectionSave> sectionSaveList;
    private List<LineSave> lineSaveList;

    public SectionAndLineDao(List<SectionSave> sectionSaveList, List<LineSave> lineSaveList) {
        this.sectionSaveList = sectionSaveList;
        this.lineSaveList = lineSaveList;
    }

}
