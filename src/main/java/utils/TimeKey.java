package utils;

import scala.Serializable;

import java.util.Date;
import java.util.Objects;

public class TimeKey implements Serializable {
    private String startTime;
    private String endTime;

    public TimeKey(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public static TimeKey getOnlyTime(TimeKey timeKey) {
        String startTime = timeKey.getStartTime().split(" ")[1];
        String endTime = timeKey.getEndTime().split(" ")[1];
        return new TimeKey(startTime, endTime);
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return "TimeKey{" +
                "startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeKey timeKey = (TimeKey) o;
        return Objects.equals(startTime, timeKey.startTime) &&
                Objects.equals(endTime, timeKey.endTime);
    }

    /**
     * 给定某个时间点，以及某个时间点的增量（单位秒）还有时段分隔单位，即可得到timeKey对象
     *
     * @param time     时间点
     * @param seconds  时间增量
     * @param interval 时段分隔单位
     * @return 时段对象
     */
    public static TimeKey getTimeKeyOfOneTimeAdd(String time, int seconds, int interval) {
        String keyStart = timeAddAndBetweenInterval(time, seconds, interval);
        String keyEnd = startTimeAddToEnd(keyStart, interval);
        return new TimeKey(keyStart, keyEnd);
    }

    public static String timeAddAndBetweenInterval(String startTime, int seconds, int timeInterval) {
        String timeAddition = DateExtendUtil.timeAdditionSecond(startTime, seconds);
        Date myDate = DateExtendUtil.stringToDate(timeAddition, DateExtendUtil.FULL);
        Date reference = DateExtendUtil.stringToDate(startTime.split(" ")[0], DateExtendUtil.PART);
        return DateExtendUtil.dateToInterval(myDate, reference, timeInterval);
    }

    public static String startTimeAddToEnd(String startTime, int timeInterval) {
        return DateExtendUtil.timeAddition(startTime, 0, timeInterval);
    }


    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }

}
