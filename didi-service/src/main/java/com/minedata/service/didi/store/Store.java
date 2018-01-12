package com.minedata.service.didi.store;

import java.io.InputStream;

import org.apache.hadoop.fs.Path;

import com.minedata.didi.DidiEntity;

public interface Store<T> {

    T getFs();

    void store(String take) throws Exception;

    boolean storeHDFS(Path take, DidiEntity didiEntity) throws Exception;

    void appendFile(InputStream sin, String dpath) throws Exception;

    void putFile(InputStream sin, String dpath) throws Exception;

    boolean isExists(String path) throws Exception;

    void mkdir() throws Exception;

    boolean deleteFile(String path) throws Exception;

    void compressFile(String path) throws Exception;

    InputStream uncompressFile(String path) throws Exception;

}
