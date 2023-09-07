package github.io.pedrogao.mq.exception;

public class StubException extends RuntimeException {

    public StubException(String message) {
        super(message);
    }

    public StubException(String message, Throwable cause) {
        super(message, cause);
    }

    public StubException(String message, int code) {
        super("Code: " + code + ", " + message);
    }

    public StubException(String message, int code, Throwable cause) {
        super("Code: " + code + ", " + message, cause);
    }
}
