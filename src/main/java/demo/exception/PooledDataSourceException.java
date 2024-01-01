package demo.exception;

public class PooledDataSourceException extends RuntimeException {

    public PooledDataSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
