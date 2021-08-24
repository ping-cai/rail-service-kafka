package exception;

import flowdistribute.OdData;
import flowdistribute.OdWithTime;

public class PathNotHaveException extends RuntimeException {
    private OdData odWithTime;
    public PathNotHaveException(String message) {
        super(message);
    }

    public PathNotHaveException(String message, OdData odWithTime) {
        super(message);
        this.odWithTime = odWithTime;
    }

    public OdData getOdWithTime() {
        return odWithTime;
    }

}
