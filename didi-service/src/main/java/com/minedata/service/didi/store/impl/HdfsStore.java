package com.minedata.service.didi.store.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.minedata.didi.DidiEntity;
import com.minedata.service.didi.store.DiDiDecompress;
import com.minedata.service.didi.store.Store;


public class HdfsStore implements Store<FileSystem> {

    private final static Logger log = Logger.getLogger(HdfsStore.class);
    public FileSystem hdfs = null;
    private Configuration conf;
    private static String PARENT_PATH;
    public static final String COPYING_FLAG = "_ing";
    public static String FINISH = "finish";
    private static String HDFS_URL;
    private int fileNum = 0;
    private String fileName = null;
    private String d = null;


    public HdfsStore(Configuration conf) {
        this.conf = conf;
        PARENT_PATH = conf.get("hdfs_didi_store");
        HDFS_URL = conf.get("hdfs_url");
        conf.setBoolean("dfs.support.append", true);
        conf.set("io.compression.codecs", LzopCodec.class.getName());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.defaultFS", HDFS_URL);
        // conf.set("mapreduce.output.fileoutputformat.compress.codec",
        // "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzopCodec");
        // conf.set("mapreduce.map.output.compress", "true");
        conf.set("io.compression.codecs",
                "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("创建hdfs存储成功");
    }

    @Override
    public FileSystem getFs() {
        return hdfs;
    }

    @Override
    public void store(String take) throws Exception {
        boolean isFinish = FINISH.equals(take);
        if (!isFinish) {
            String[] record = take.split("/");
            fileName = record[record.length - 2];
            d = PARENT_PATH + "/" + fileName + COPYING_FLAG;
            log.info("要存储的目录" + d);
        }
        if (!isFinish && take != null) {// 等于FINISH时，获取队列数据超时；证明已经传输完成，或者后续数据无法传输
            if (!isExists(d + ".lzo")) {// 如果已经压缩完成，不在向该文件append文件
                try {
                    File uncompress = DiDiDecompress.gz2file(take);
                    InputStream sin = new FileInputStream(uncompress);
                    if (!isExists(d)) {
                        if (!isExists(PARENT_PATH)) {
                            mkdir();
                        }
                        putFile(sin, d);
                    } else {
                        appendFile(sin, d);
                    }
                    ++fileNum;
                    log.info("拷贝文件个数: " + fileNum + " 当前拷贝文件: " + take);
                    uncompress.delete();
                } catch (Exception e) {
                    log.error(take + "文件解压失败！");
                }
            }
        } else if (isFinish) {
            String s1 = PARENT_PATH + "/" + fileName;
            log.info("文件拷贝完成，修改文件名称从 " + d + " 修改为 " + s1);
            hdfs.rename(new Path(d), new Path(s1));

            // 执行文件压缩 start
            // log.info(d + " 文件开始压缩");
            gzCompressFile(s1);
            // log.info(d + " 文件压缩完成");
            // 执行文件压缩 end

            // mapreduce 分省市 start
            // DidiUserPathPreprocess.main(new String[]{"hdfs://xdatanode-05/" + s1,
            // "hdfs://xdatanode-05/dm/trajectory/didi_rt/user_traj_test"});
            // mapreduce 分省市 end

            fileNum = 0;

            // if(isProvinceComplete(fileName)){
            boolean flag = deleteFile(s1);
            log.info(d + " 文件删除 " + (flag ? "成功" : "失败"));
            log.info(d + "文件拷贝到hdfs完成");
            // }else{
            // log.error("分省程序执行失败");
            // }

        }
    }



    public void putFile(InputStream sin, String dpath) throws Exception {
        OutputStream out = hdfs.create(new Path(dpath));
        IOUtils.copyBytes(sin, out, 10 * 1024 * 1024, true);
    }

    public void appendFile(InputStream sin, String dpath) throws Exception {
        hdfs = FileSystem.get(URI.create(dpath), conf);
        OutputStream out = hdfs.append(new Path(dpath));
        IOUtils.copyBytes(sin, out, 10 * 1024 * 1024, true);
    }

    public boolean isExists(String path) throws Exception {
        return hdfs.exists(new Path(path));
    }

    public void mkdir() throws Exception {
        hdfs.mkdirs(new Path(PARENT_PATH));
    }

    public boolean deleteFile(String path) throws Exception {
        return hdfs.delete(new Path(path), false);
    }


    public void compressFile(String path) throws Exception {
        CompressionCodec lzo = ReflectionUtils.newInstance(LzopCodec.class, conf);
        Path p = new Path(path + lzo.getDefaultExtension());
        InputStream in = hdfs.open(new Path(path));
        OutputStream out = lzo.createOutputStream(hdfs.create(p));
        IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        LzoIndex.createIndex(hdfs, p);

    }

    public void gzCompressFile(String path) throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Path p = new Path(path + codec.getDefaultExtension());
        InputStream in = hdfs.open(new Path(path));
        OutputStream out = codec.createOutputStream(hdfs.create(p));
        IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
    }

    public void uncompressZipFile(String intputpath, String outputPath)
            throws ClassNotFoundException, IllegalArgumentException, IOException {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = hdfs.open(new Path(intputpath));
        InputStream in = codec.createInputStream(inputStream);
        FSDataOutputStream outputStream = hdfs.create(new Path(outputPath));
        IOUtils.copyBytes(in, outputStream, 10 * 1024 * 1024, true);
    }

    public InputStream uncompressFile(String path) throws Exception {
        return new GZIPInputStream(new FileInputStream(path));
    }


    // public void storeHDFS(Path take) throws Exception {
    // boolean isFinish = FINISH.equals(take);
    // if (!isFinish) {
    // fileName = take.getParent().getParent().getParent().getName();
    // d = PARENT_PATH + "/" + fileName + COPYING_FLAG;
    // log.info("正在合并到文件" + d);
    // }
    // if (!isFinish && take != null) {// 等于FINISH时，获取队列数据超时；证明已经传输完成，或者后续数据无法传输
    // if (!isExists(d + ".lzo")) {// 如果已经压缩完成，不在向该文件append文件
    // try {
    // if (!isExists(PARENT_PATH)) {
    // mkdir();
    // }
    // DiDiDecompress.gz2hdfs(conf, take.toString(), d);
    // deleteFile(d + "_gzing");
    // ++fileNum;
    // log.info("拷贝文件个数: " + fileNum + " 当前拷贝文件: " + take);
    // } catch (Exception e) {
    // log.error(take + "文件解压失败！");
    // }
    // }
    // } else if (isFinish) {
    // String s1 = PARENT_PATH + "/" + fileName;
    // log.info("文件拷贝完成，修改文件名称从 " + d + " 修改为 " + s1);
    // hdfs.rename(new Path(d), new Path(s1));
    // // 执行文件压缩 start
    // // log.info(d + " 文件开始压缩");
    // gzCompressFile(s1);
    // // log.info(d + " 文件压缩完成");
    // // 执行文件压缩 end
    //
    // // mapreduce 分省市 start
    // // DidiUserPathPreprocess.main(new String[]{"hdfs://xdatanode-05/" + s1,
    // // "hdfs://xdatanode-05/dm/trajectory/didi_rt/user_traj_test"});
    // // mapreduce 分省市 end
    //
    // fileNum = 0;
    // // if(isProvinceComplete(fileName)){
    // boolean flag = deleteFile(s1);
    //
    // log.info(d + " 文件删除 " + (flag ? "成功" : "失败"));
    // log.info(d + "文件拷贝到hdfs完成");
    // // }else{
    // // log.error("分省程序执行失败");
    // // }
    //
    // }
    // }

    @Override
    public boolean storeHDFS(Path take, DidiEntity didiEntity) throws Exception {
        boolean isFinish = FINISH.equals(take.getName());
        String version = didiEntity.getVersion();
        String region = didiEntity.getRegion();
        // if (!isFinish) {
        if (region == null || "".equals(region)) {
            region = take.getParent().getParent().getName();
        }
        if (version == null || "".equals(version)) {
            fileName = take.getParent().getParent().getParent().getName();
        } else {
            fileName = version;
        }

        if (!hdfs.exists(new Path(PARENT_PATH + "/" + region))) {
            hdfs.mkdirs(new Path(PARENT_PATH + "/" + region));
        }
        d = PARENT_PATH + "/" + region + "/" + fileName + COPYING_FLAG;
        log.info("正在合并到文件" + d);
        // }
        if (!isFinish && take != null) {// 等于FINISH时，获取队列数据超时；证明已经传输完成，或者后续数据无法传输
            if (!isExists(d + ".gz")) {// 如果已经压缩完成，不在向该文件append文件
                try {
                    if (!isExists(PARENT_PATH)) {
                        mkdir();
                    }
                    DiDiDecompress.gz2hdfs(conf, take.toString(), d);
                    deleteFile(d + "_gzing");
                    ++fileNum;
                    log.info("拷贝文件个数: " + fileNum + " 当前拷贝文件: " + take);
                } catch (Exception e) {
                    log.error(take + "文件解压失败！");
                }
            }
        } else if (isFinish) {
            String s1 = PARENT_PATH + "/" + region + "/" + fileName;
            log.info("文件拷贝完成，修改文件名称从 " + d + " 修改为 " + s1);
            didiEntity.setPath(s1);
            hdfs.rename(new Path(d), new Path(s1));
            // 执行文件压缩 start
            // log.info(d + " 文件开始压缩");
            // gzCompressFile(s1);
            // log.info(d + " 文件压缩完成");
            // 执行文件压缩 end

            // mapreduce 分省市 start
            // DidiUserPathPreprocess.main(new String[]{"hdfs://xdatanode-05/" + s1,
            // "hdfs://xdatanode-05/dm/trajectory/didi_rt/user_traj_test"});
            // mapreduce 分省市 end

            // fileNum = 0;
            // if(isProvinceComplete(fileName)){
            // boolean flag = deleteFile(s1);
            // log.info(d + " 文件删除 " + (flag ? "成功" : "失败"));
            log.info(d + "文件拷贝到hdfs完成");
            // }else{
            // log.error("分省程序执行失败");
            // }

        }
        return isFinish;
    }

    public void mkdir(String outputPath) {
        try {
            hdfs.mkdirs(new Path(outputPath));
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
    }

}
