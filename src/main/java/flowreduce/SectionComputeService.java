package flowreduce;

import dao.SectionAndLineDao;
import domain.Section;
import domain.TravelTimeAndRate;
import entity.GraphWithSectionAssociation;
import exception.InvalidPathException;
import flowdistribute.LineWithFlow;
import flowdistribute.OdWithTime;
import flowdistribute.SimpleLogitResult;
import kspcalculation.*;
import utils.TimeKey;

import java.io.Serializable;
import java.util.*;

public class SectionComputeService implements Serializable {
    //    换乘消耗时间,单位秒
    public static final double TRANSFER_TIME = 5.0 * 60;
    //    车站停车时间，单位秒
    public static final double STATION_STOP_TIME = 0.75 * 60;
    //    时段间隔，单位分钟
    public static final int TIME_INTERVAL = 15;

    public static SectionResult getSectionBylogitResult(SimpleLogitResult simpleLogitResult, GraphWithSectionAssociation graphWithSectionAssociation) {
        SectionAssociationMap sectionAssociationMap = graphWithSectionAssociation.getAssociationMap();
        Map<Section, TravelTimeAndRate> sectionTravelTimeAndRateMap = graphWithSectionAssociation.getSectionTravelTimeAndRateMap();
        OdWithTime odWithTime = simpleLogitResult.getOdWithTime();
        String inTime = odWithTime.getInTime();
        Map<Path, Double> logitResult = simpleLogitResult.getLogitResult();
        Map<TimeKey, Map<SimpleSection, Double>> simpleSectionMap = new HashMap<>(300);
        Map<TimeKey, LineResult> lineResultMap = new HashMap<>(300);
        Map<TimeKey, Map<SimpleTransfer, Double>> simpleTransferMap = new HashMap<>(100);
        for (Map.Entry<Path, Double> logitPath : logitResult.entrySet()) {
//            初始化每条路径的初始时间
            double pathIncrementTime = 0.0;
            Path path = logitPath.getKey();
            Double flow = logitPath.getValue();
            LinkedList<Edge> edges = path.getEdges();
            int size = edges.size();
            if (size < 1) {
                throw new InvalidPathException("invalid path!the path not have edges,please check the travel graph!");
            }
            for (int i = 0; i < size; i++) {
                Edge edge = edges.get(i);
                Section section = Section.createSectionByEdge(edge);
                TravelTimeAndRate travelTimeAndRate = sectionTravelTimeAndRateMap.get(section);
//                为空代表则换乘站，加上换乘时间
                if (travelTimeAndRate == null) {
                    pathIncrementTime += TRANSFER_TIME;
                    continue;
                }
                Integer seconds = travelTimeAndRate.getSeconds();
//                这里加上区间运行时间
                pathIncrementTime += seconds;
                String startTime = TimeKey.timeAddAndBetweenInterval(inTime, (int) pathIncrementTime, TIME_INTERVAL);
                String endTime = TimeKey.startTimeAddToEnd(startTime, TIME_INTERVAL);
                SectionAllInfo sectionInfo = sectionAssociationMap.getSectionInfo(edge);
                Integer sectionId = sectionInfo.getSectionId();
                String lineName = sectionAssociationMap.getLineName(edge);
                TimeKey timeKey = new TimeKey(startTime, endTime);
                SimpleSection simpleSection = new SimpleSection(sectionId, edge.getFromNode(), edge.getToNode());
                addElementToSimpleSectionMap(simpleSectionMap, flow, timeKey, simpleSection);
                addElementToLineResultMap(lineResultMap, flow, lineName, timeKey);
                addElementToSimpleTransferMap(sectionAssociationMap, simpleTransferMap, flow, edges, i, edge, sectionInfo, timeKey);
//                这里加上车站停车时间
                pathIncrementTime += STATION_STOP_TIME;
            }
        }
        return new SectionResult(simpleSectionMap, lineResultMap, simpleTransferMap);
    }

    private static void addElementToSimpleTransferMap(SectionAssociationMap sectionAssociationMap, Map<TimeKey, Map<SimpleTransfer, Double>> simpleTransferMap, Double flow, LinkedList<Edge> edges, int i, Edge edge, SectionAllInfo sectionInfo, TimeKey timeKey) {
        String direction = sectionInfo.getDirection();
        if ("0".equals(direction)) {
            Edge beforeEdge = edges.get(i - 1);
            String beforeDirection = sectionAssociationMap.getSectionInfo(beforeEdge).getDirection();
            Edge afterEdge = edges.get(i + 1);
            String afterDirection = sectionAssociationMap.getSectionInfo(afterEdge).getDirection();
            sectionAssociationMap.getSectionInfo(edge);
            if (simpleTransferMap.containsKey(timeKey)) {
                Map<SimpleTransfer, Double> transferFlowMap = simpleTransferMap.get(timeKey);
                SimpleTransfer simpleTransfer = new SimpleTransfer(edge, beforeDirection, afterDirection);
                if (transferFlowMap.containsKey(simpleTransfer)) {
                    Double transferFlow = transferFlowMap.get(simpleTransfer);
                    transferFlowMap.put(simpleTransfer, transferFlow + flow);
                } else {
                    transferFlowMap.put(simpleTransfer, flow);
                }
            } else {
                SimpleTransfer simpleTransfer = new SimpleTransfer(edge, beforeDirection, afterDirection);
                Map<SimpleTransfer, Double> transferFlowMap = new HashMap<>();
                transferFlowMap.put(simpleTransfer, flow);
                simpleTransferMap.put(timeKey, transferFlowMap);
            }
        }
    }

    private static void addElementToLineResultMap(Map<TimeKey, LineResult> lineResultMap, Double flow, String lineName, TimeKey timeKey) {
        if (lineResultMap.containsKey(timeKey)) {
            LineResult lineResult = lineResultMap.get(timeKey);
            Map<String, Double> resultLineFlowMap = lineResult.getLineFlowMap();
            if (resultLineFlowMap.containsKey(lineName)) {
                Double lineFlow = resultLineFlowMap.get(lineName);
                resultLineFlowMap.put(lineName, lineFlow + flow);
            } else {
                resultLineFlowMap.put(lineName, flow);
            }
            lineResultMap.put(timeKey, lineResult);
        } else {
            Map<String, Double> lineFlowMap = new HashMap<>(30);
            lineFlowMap.put(lineName, flow);
            LineResult lineResult = new LineResult(lineFlowMap);
            lineResultMap.put(timeKey, lineResult);
        }
    }

    private static void addElementToSimpleSectionMap(Map<TimeKey, Map<SimpleSection, Double>> simpleSectionMap, Double flow, TimeKey timeKey, SimpleSection simpleSection) {
        if (simpleSectionMap.containsKey(timeKey)) {
            Map<SimpleSection, Double> flowMap = simpleSectionMap.get(timeKey);
            if (flowMap.containsKey(simpleSection)) {
                Double passenger = flowMap.get(simpleSection);
                flowMap.put(simpleSection, passenger + flow);
            } else {
                flowMap.put(simpleSection, flow);
            }
        } else {
            Map<SimpleSection, Double> flowMap = new HashMap<>(30);
            flowMap.put(simpleSection, flow);
            simpleSectionMap.put(timeKey, flowMap);
        }
    }

    public static List<LineWithFlow> getLineLogitResult(SimpleLogitResult simpleLogitResult, SectionAssociationMap sectionAssociationMap) {
        Map<Path, Double> logitResult = simpleLogitResult.getLogitResult();

        List<LineWithFlow> withFlowList = new ArrayList<>();
        for (Map.Entry<Path, Double> logitPath : logitResult.entrySet()) {
            Path path = logitPath.getKey();
            Double flow = logitPath.getValue();
            LinkedList<Edge> edges = path.getEdges();
            Set<String> lines = new HashSet<String>();
            for (Edge edge : edges) {
                String lineName = sectionAssociationMap.getLineName(edge);
                lines.add(lineName);
            }
            LineWithFlow lineWithFlow = new LineWithFlow(lines, flow);
            withFlowList.add(lineWithFlow);
        }
        return withFlowList;
    }

    public static List<SectionSave> sectionSave(Map<TimeKey, Map<SimpleSection, Double>> sectionComputeServiceResult) {
        List<SectionSave> sectionSaveList = new ArrayList<>();
        List<LineSave> lineSaveList = new ArrayList<>();
        for (TimeKey timeKey : sectionComputeServiceResult.keySet()) {
            Map<SimpleSection, Double> simpleSectionMap = sectionComputeServiceResult.get(timeKey);
            for (SimpleSection simpleSection : simpleSectionMap.keySet()) {
                float flow = simpleSectionMap.get(simpleSection).floatValue();
                SectionSave sectionSave = new SectionSave(timeKey.getStartTime(), timeKey.getEndTime(), simpleSection.getSectionId(), simpleSection.getInId(), simpleSection.getOutId(), flow);
                sectionSaveList.add(sectionSave);
            }
        }
        return sectionSaveList;
    }

    public static List<LineSave> lineSave(Map<TimeKey, LineResult> lineResultMap) {
        List<LineSave> lineSaveList = new ArrayList<>();
        for (TimeKey timeKey : lineResultMap.keySet()) {
            LineResult lineResult = lineResultMap.get(timeKey);
            Map<String, Double> lineFlowMap = lineResult.getLineFlowMap();
            for (String lineName : lineFlowMap.keySet()) {
                float flow = lineFlowMap.get(lineName).floatValue();
                LineSave lineSave = new LineSave(timeKey, lineName, flow);
                lineSaveList.add(lineSave);
            }
        }
        return lineSaveList;
    }

    public static SectionAndLineDao sectionResult(SectionResult sectionResult) {
        List<SectionSave> sectionSaveList = sectionSave(sectionResult.getSimpleSectionMap());
        List<LineSave> lineSaveList = lineSave(sectionResult.getLineResultMap());
        return new SectionAndLineDao(sectionSaveList, lineSaveList);
    }

}
