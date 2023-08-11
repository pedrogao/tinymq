package github.io.pedrogao.fqueue.exception;

public class FileEOFException extends Exception {
    public FileEOFException(String message) {
        super(message);
    }

    public FileEOFException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileEOFException(Throwable cause) {
        super(cause);
    }
}
