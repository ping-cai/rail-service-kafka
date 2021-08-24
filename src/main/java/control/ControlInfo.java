package control;

import conf.DynamicConf;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ControlInfo implements Serializable {
    private String odSourcePath;
    private String lineTable = String.format("LINE_%s", startupTime());
    private String sectionTable = String.format("SECTION_%s", startupTime());
    private String transferTable = String.format("TRANSFER_%s", startupTime());
    private String stationTable = String.format("STATION_%s", startupTime());
    private String odWithSectionSavePath = String.format("%s/OD_WITH_SECTION_%s", "hdfs://hacluster", startupTime());
    private String distributionType = DynamicConf.distributionType();
    private Integer kspNumber = DynamicConf.pathNum();
    private Integer timeInterval = DynamicConf.timeInterval();
    private boolean openShareFunction = DynamicConf.openShareFunction();

    public ControlInfo(String odSourcePath) {
        this.odSourcePath = odSourcePath;
    }

    public ControlInfo(String odSourcePath, Integer kspNumber) {
        this.odSourcePath = odSourcePath;
        this.kspNumber = kspNumber;
    }

    public ControlInfo(String odSourcePath, String distributionType, Integer kspNumber) {
        this.odSourcePath = odSourcePath;
        this.distributionType = distributionType;
        this.kspNumber = kspNumber;
    }

    public ControlInfo(String odSourcePath, String distributionType, Integer kspNumber, Integer timeInterval) {
        this.odSourcePath = odSourcePath;
        this.distributionType = distributionType;
        this.kspNumber = kspNumber;
        this.timeInterval = timeInterval;
    }

    public static String startupTime() {
        return new SimpleDateFormat("yyyy_MM_dd_HH").format(new Date());
    }

    public String getOdSourcePath() {
        return odSourcePath;
    }

    public String getSectionTable() {
        return sectionTable;
    }

    public String getTransferTable() {
        return transferTable;
    }

    public String getStationTable() {
        return stationTable;
    }

    public String getDistributionType() {
        return distributionType;
    }

    public Integer getTimeInterval() {
        return timeInterval;
    }

    public Integer getKspNumber() {
        return kspNumber;
    }

    public String getLineTable() {
        return lineTable;
    }

    public String getOdWithSectionSavePath() {
        return odWithSectionSavePath;
    }

    public void setOdSourcePath(String odSourcePath) {
        this.odSourcePath = odSourcePath;
    }

    public void setLineTable(String lineTable) {
        this.lineTable = lineTable;
    }

    public void setSectionTable(String sectionTable) {
        this.sectionTable = sectionTable;
    }

    public void setTransferTable(String transferTable) {
        this.transferTable = transferTable;
    }

    public void setStationTable(String stationTable) {
        this.stationTable = stationTable;
    }

    public void setDistributionType(String distributionType) {
        this.distributionType = distributionType;
    }

    public void setKspNumber(Integer kspNumber) {
        this.kspNumber = kspNumber;
    }

    public void setTimeInterval(Integer timeInterval) {
        this.timeInterval = timeInterval;
    }

    public void setOdWithSectionSavePath(String odWithSectionSavePath) {
        this.odWithSectionSavePath = odWithSectionSavePath;
    }

    public boolean isOpenShareFunction() {
        return openShareFunction;
    }

    public void setOpenShareFunction(boolean openShareFunction) {
        this.openShareFunction = openShareFunction;
    }
}
