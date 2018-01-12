package com.minedata.config;

import lombok.Data;

@Data
public class ServiceConfig {

    public boolean isThrowException() {
        return throwException;
    }

    public void setThrowException(boolean throwException) {
        this.throwException = throwException;
    }

    /**
     * service provider发生错误时,是否抛出异常
     */
    private boolean throwException = false;
}
