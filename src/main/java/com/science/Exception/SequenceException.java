package com.science.Exception;

/**
 * Created by zhaixiaotong on 2017-1-11.
 */
public class SequenceException extends Exception {
    private static final long serialVersionUID = 1L;

    public SequenceException() {
    }

    public SequenceException(String message) {
        super(message);
    }

    public SequenceException(String message, Throwable cause) {
        super(message, cause);
    }

    public SequenceException(Throwable cause) {
        super(cause);
    }
}
