package exception;

public class SectionNotExistException extends RuntimeException {
    public static final String MESSAGE = "There is no such section, please check if there is any problem in the superposition of section flow table";

    public SectionNotExistException(String message) {
        super(message);
    }
}
