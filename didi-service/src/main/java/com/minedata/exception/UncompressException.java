package com.minedata.exception;

public class UncompressException extends BaseException {
    private static final long serialVersionUID = -3711290613973933714L;

    public UncompressException(String message, Throwable cause, String code, Object[] values) {
        super(message, cause, code, values);
        // TODO Auto-generated constructor stub
    }

    public UncompressException(String code) {
        super(code, null, code, null);
    }

    public UncompressException(Throwable cause, String code) {
        super(code, cause, code, null);
    }

    public UncompressException(String code, Object[] values) {
        super(code, null, code, values);
    }

    public UncompressException(Throwable cause, String code, Object[] values) {
        super(code, null, code, values);
    }
}
