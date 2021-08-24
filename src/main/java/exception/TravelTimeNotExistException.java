package exception;

public class TravelTimeNotExistException extends RuntimeException {
    public static final String MESSAGE = "The running time of the train in this section cannot be found. Please check whether there is A problem with the running time diagram of the section. The section you pass is";

    public TravelTimeNotExistException() {
    }

    public TravelTimeNotExistException(String message) {
        super(message);
    }

    public TravelTimeNotExistException(String message, Throwable cause) {
        super(message, cause);
    }

    public TravelTimeNotExistException(Throwable cause) {
        super(cause);
    }

    public TravelTimeNotExistException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
