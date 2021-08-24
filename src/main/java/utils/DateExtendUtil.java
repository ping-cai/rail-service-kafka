package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author LiYongPing
 * 转换日期格式类
 */
public class DateExtendUtil {
    public static final String FULL = "yyyy-MM-dd HH:mm:ss";
    //    精确度一天
    private static final long ONE_DAY = 24 * 60 * 60 * 1000L;
    public static final String PART = "yyyy-MM-dd";
    //    精确度一秒
    public static final long SECEND = 1000L;
    //    精确度一分钟
    public static final long MINUTE = 1000 * 60L;
    //    精确度一小时
    public static final long HOUR = 1000 * 60 * 60;

    public static final String YEAR = "year";
    public static final String MONTH = "month";


    /**
     * 类似日期字符串转换为Date类型
     *
     * @param strDate   类似于 "2018-01-09 00:00:00" 或者 "2018-01-09"
     * @param formatStr 选择 FULL "yyyy-MM-dd HH:mm:ss" 或者 PART "yyyy-MM-dd"
     * @return 格林威治时间，Date类型
     */
    public static Date stringToDate(String strDate, String formatStr) {
        Date date = null;
        if (formatStr != null && !"".equals(formatStr)) {
            SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
            try {
                date = sdf.parse(strDate);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return date;
    }

    /**
     * @param myDate        传入待划分时间间隔的时间
     * @param referenceTime 参照时间，这个时间通常为待划分时间的凌晨零点时间
     * @param interval      时间间隔，15分钟，30分钟，60分钟等
     * @return 返回时间段字符串
     * @Date 2021-03-02
     */
    public static String dateToInterval(Date myDate, Date referenceTime, int interval) {
        int dateDiff = dateDiff(referenceTime, myDate, MINUTE);
        int intervalNumbers = dateDiff / interval;
        int spaceTime = intervalNumbers * interval;
        Date date = dateAddition(referenceTime, 0, spaceTime);
        return dateToString(date);
    }

    public static String timeToDatetime(String time, String referenceDatetime) {
        String[] timeArray = time.split(".");
        String hours = timeArray[0];
        String minutes = timeArray[1];
        String referenceDate = referenceDatetime.split(" ")[0];
        Date date = stringToDate(referenceDate, PART);
        Date finalDatetime = dateAddition(date, Integer.parseInt(hours), Integer.parseInt(minutes));
        return dateToString(finalDatetime);
    }


    /**
     * @param strDate 非标准日期字符串时间，类似2018/1/2,或者2018/01/02
     * @return 标准日期格式时间，2018-01-02 00:00:00
     */
    public static String standardToString(String strDate) {
        try {
            String[] split = strDate.split("/");
            int month = Integer.parseInt(split[1]);
            int day = Integer.parseInt(split[2]);
            if (month < 10 || day < 10) {
                if (month < 10) {
                    split[1] = "0" + month;
                    if (Integer.parseInt(split[2]) < 10) {
                        split[2] = "0" + day;
                    }
                } else {
                    if (Integer.parseInt(split[2]) < 10) {
                        split[2] = "0" + day;
                    }
                }
                return split[0] + "-" + split[1] + "-" + split[2] + " 00:00:00";
            } else {
                return split[0] + "-" + split[1] + "-" + split[2] + " 00:00:00";
            }
        } catch (Exception ignored) {
        }
        return strDate;
    }

    /**
     * @param date 格林威治时间
     * @return 格林威治时间的标准日期显示格式 2018-01-02 00:00:00
     */
    public static String dateToString(Date date) {
        SimpleDateFormat standardTime = new SimpleDateFormat(FULL);
        return standardTime.format(date);
    }

    /**
     * String类型的时间增量
     *
     * @param startTime 开始时间
     * @param hour      经过的小时数
     * @param minute    经过的分钟数
     * @return 最后时间
     */
    public static String timeAddition(String startTime, int hour, int minute) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date start = dateFormat.parse(startTime);
            calendar.setTime(start);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        calendar.add(Calendar.HOUR, hour);
        calendar.add(Calendar.MINUTE, minute);
        Date time = calendar.getTime();
        return dateFormat.format(time);
    }

    /**
     * Date类型的时间增量
     *
     * @param startTime 开始时间
     * @param hour      经过的小时数
     * @param minute    经过的分钟数
     * @return 最后时间
     */
    public static Date dateAddition(Date startTime, int hour, int minute) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startTime);
        calendar.add(Calendar.HOUR, hour);
        calendar.add(Calendar.MINUTE, minute);
        return calendar.getTime();
    }

    /**
     * String类型以秒数为增量的方法
     *
     * @param startTime 开始时间
     * @param second    经过的秒钟数
     * @return 最后时间
     */
    public static String timeAdditionSecond(String startTime, int second) {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date start = dateFormat.parse(startTime);
            calendar.setTime(start);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        calendar.add(Calendar.SECOND, second);
        Date time = calendar.getTime();
        return dateFormat.format(time);
    }

    /**
     * Date类型以秒数为增量的方法
     *
     * @param startTime 开始时间
     * @param second    经过的秒数
     * @return 最后时间
     */
    public static Date dateAdditionSecond(Date startTime, int second) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startTime);
        calendar.add(Calendar.SECOND, second);
        return calendar.getTime();
    }

    /**
     * String类型求取两个时间的差值
     *
     * @param start 开始时间
     * @param end   终止时间
     * @param range 差值单位
     * @return 差值(int)
     */
    public static int timeDifference(String start, String end, long range) {
        Date date1 = stringToDate(start, FULL);
        Date date2 = stringToDate(end, FULL);
        long l = date2.getTime() - date1.getTime();
        return (int) (l / range);
    }

    /**
     * Date类型求取两个时间的差值
     *
     * @param date1 开始时间
     * @param date2 终止时间
     * @param range 差值单位
     * @return 差值(int)
     */
    public static int dateDiff(Date date1, Date date2, long range) {
        long l = date2.getTime() - date1.getTime();
        return (int) (l / range);
    }

    public static int strDateGet(String strTime, String field) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(DateExtendUtil.stringToDate(strTime, DateExtendUtil.FULL));
        if (YEAR.equals(field)) {
            return calendar.get(Calendar.YEAR);
        } else if (MONTH.equals(field)) {
            return calendar.get(Calendar.MONTH) + 1;
        } else {
            return calendar.get(Calendar.DATE);
        }
    }

    public static void main(String[] args) {
        System.out.println(DateExtendUtil.timeDifference("2021-02-24 20:59:00", "2021-02-25 21:00:00", DateExtendUtil.HOUR));
    }
}
