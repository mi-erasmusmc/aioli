package nl.erasmusmc.mi.biosemantics.exception;

public class SqlFileToStringException extends RuntimeException {

    public SqlFileToStringException(Exception e) {
        super(e);
    }
}
