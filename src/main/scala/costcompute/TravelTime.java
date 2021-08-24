package costcompute;

import java.io.Serializable;

public class TravelTime implements Serializable {
    public static final int ONE_DAY = 24 * 60 * 60;
    private String departureTime;
    private String arrivalTime;

    public TravelTime(String departureTime, String arrivalTime) {
        try {
            checkValid(departureTime, arrivalTime);
            this.departureTime = departureTime;
            this.arrivalTime = arrivalTime;
        } catch (RuntimeException e) {
            this.departureTime = "00.00.00";
            this.arrivalTime = "00.00.00";
        }
    }

    private void checkValid(String departureTime, String arrivalTime) {
        if (departureTime.charAt(0) == '-') {
            throw new RuntimeException(String.format("departureTime is invalid and the value is %s please check the travel graph yxtKhsk", departureTime));
        }
        if (arrivalTime.charAt(0) == '-') {
            throw new RuntimeException(String.format("arrivalTime is invalid and the value is %s please check the travel graph yxtKhsk", arrivalTime));
        }
    }

    public String getDepartureTime() {
        return departureTime;
    }

    public String getArrivalTime() {
        return arrivalTime;
    }

    public int getSeconds() {
        return getSeconds(getDepartureTime(), getArrivalTime());
    }

    public static int getSeconds(String startTime, String endTime) {
        String[] endTimeArray = endTime.split("\\.");
        String[] startTimeArray = startTime.split("\\.");
        int hours = 0;
        try {
            hours = Integer.parseInt(endTimeArray[0]) - Integer.parseInt(startTimeArray[0]);
        } catch (NumberFormatException e) {
            return hours;
        }
        int minutes = Integer.parseInt(endTimeArray[1]) - Integer.parseInt(startTimeArray[1]);
        int seconds = Integer.parseInt(endTimeArray[2]) - Integer.parseInt(startTimeArray[2]);
        int result = hours * 3600 + minutes * 60 + seconds;
        if (result < 0) {
            result += ONE_DAY;
        }
        return result;
    }

    @Override
    public String toString() {
        return "TravelTime{" +
                "departureTime='" + departureTime + '\'' +
                ", arrivalTime='" + arrivalTime + '\'' +
                '}';
    }
}
