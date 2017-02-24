package com.science.Exception;

/**
 * Created by zhaixiaotong on 2017-1-11.
 */
public class UniqueKeyException extends Exception {
    private static final long serialVersionUID = 1L;

    public UniqueKeyException() {
    }

    public UniqueKeyException(String message) {
        super(message);
    }

    public UniqueKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public UniqueKeyException(Throwable cause) {
        super(cause);
    }
}
