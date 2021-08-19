package nl.erasmusmc.mi.biosemantics.exception;

public class DatabaseException extends RuntimeException {


    public DatabaseException(Exception e) {
        super(e);
    }

}
