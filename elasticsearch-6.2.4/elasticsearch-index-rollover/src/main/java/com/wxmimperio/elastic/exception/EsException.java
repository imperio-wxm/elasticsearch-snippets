package com.wxmimperio.elastic.exception;

public class EsException extends Exception {

    public EsException(String message) {
        super(message);
    }

    public EsException(String message, Throwable cause) {
        super(message, cause);
    }
}
