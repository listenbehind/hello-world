package com.minedata.exception;

public class RestServiceError {
    private String errcode;
    private String errmsgs;
    private String content;


    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getErrcode() {
        return errcode;
    }

    public void setErrcode(String errcode) {
        this.errcode = errcode;
    }

    public String getErrmsgs() {
        return errmsgs;
    }

    public void setErrmsgs(String errmsgs) {
        this.errmsgs = errmsgs;
    }

    public static RestServiceError build(Type errorType, String message) {
        RestServiceError error = new RestServiceError();
        error.errcode = errorType.getCode();
        error.errmsgs = message;
        error.content = "null";
        return error;
    }

    public enum Type {
        BAD_REQUEST_ERROR("505", "Bad request error"), INTERNAL_SERVER_ERROR("500",
                "Unexpected server error"), VALIDATION_ERROR("error.validation",
                "Found validation issues");

        private String code;
        private String message;

        Type(String code, String message) {
            this.code = code;
            this.message = message;
        }

        public String getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }
    }
}
