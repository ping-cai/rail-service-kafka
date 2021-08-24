package tools;

import utils.DateExtendUtil;
import utils.TimeKey;

import java.io.Serializable;

public class StationTimeHandle implements Serializable {
    /**
     * 求得特定时间间隔的对象
     *
     * @param startTime 开始时间，字符串日期格式yyyy-mm-dd hh24:mi:ss
     * @param endTime   结束时间,字符串日期格式yyyy-mm-dd hh24:mi:ss
     * @param interval  特定时间间隔
     * @return 时间间隔对象
     */
    public static TimeKey getSpecificTimePeriodOfTwoTimePoints(String startTime, String endTime, int interval) {
        int diffSeconds = DateExtendUtil.timeDifference(startTime, endTime, DateExtendUtil.SECEND);
        String middleTime = DateExtendUtil.timeAdditionSecond(startTime, diffSeconds / 2);
        String startDatePart = middleTime.split(" ")[0];
        String startDate = String.format("%s 00:00:00", startDatePart);
        int diffMinutes = DateExtendUtil.timeDifference(startDate, middleTime, DateExtendUtil.MINUTE);
        int intervalNum = diffMinutes / interval;
        int finalMinutes = intervalNum * interval;
        String resultStartTime = DateExtendUtil.timeAddition(startDate, 0, finalMinutes);
        String resultEndTime = DateExtendUtil.timeAddition(resultStartTime, 0, interval);
        return new TimeKey(resultStartTime, resultEndTime);
    }

    public static boolean containsArrivalTime(TimeKey timeKey, String arrivalTime) {
        String startTime = timeKey.getStartTime();
        String startDate = startTime.split(" ")[0];
        String[] arrivalTimeArray = arrivalTime.split("\\.");
        String hour = arrivalTimeArray[0];
        String minutes = arrivalTimeArray[1];
        String seconds = arrivalTimeArray[2];
        String checkTime = String.format("%s %s:%s:%s", startDate, hour, minutes, seconds);
        String endTime = timeKey.getEndTime();
        return DateExtendUtil.timeDifference(startTime, checkTime, DateExtendUtil.SECEND) > 0
                && DateExtendUtil.timeDifference(endTime, checkTime, DateExtendUtil.SECEND) < 0;
    }
}
