package com.minedata.uncompress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.minedata.service.didi.DidiRestServiceImpl;
import com.netflix.config.DynamicPropertyFactory;

public class DidiUncompress {
    private final static Logger log = Logger.getLogger(DidiRestServiceImpl.class);
    private static FileSystem hdfs = null;
    private static final DynamicPropertyFactory configInstance =
            com.netflix.config.DynamicPropertyFactory.getInstance();
    private ExecutorService threadPool = Executors.newFixedThreadPool(10);
    private LinkedBlockingQueue<FileStatus> compressQueue = new LinkedBlockingQueue<FileStatus>();
    private Set<FileStatus> lastSet = new HashSet<FileStatus>();

    private final static int FILE_CREATE_INTERVAL_MIN = 5;
    private final static int ALL_DAY_MIN = 24 * 60;

    public static final String TEMP_HDFS_PARENT_PATH;
    public static final String PROVINCE_HDFS_PARENT_PATH;
    public static final String ORGIN_FILE_PARENT_PATH;
    public static final String FILE_PARENT_PATH;
    public static String FINISH = "finish";
    private static final String MD5_SUFFIX = ".md5";
    private static final String HDFS_URL;
    private static int fileLen;
    private static Configuration conf = new Configuration();
    static {
        try {
            conf.addResource(new FileInputStream(new File("./env_config.xml")));
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        }


        // conf.addResource(
        // DidiRestServiceImpl.class.getClassLoader().getResourceAsStream("env_config.xml"),
        // "utf-8");
        TEMP_HDFS_PARENT_PATH = conf.get("hdfs_didi_store");
        fileLen = TEMP_HDFS_PARENT_PATH.split("/").length;
        PROVINCE_HDFS_PARENT_PATH = conf.get("hdfs_didi_province");
        FILE_PARENT_PATH = conf.get("local_didi_upload");
        ORGIN_FILE_PARENT_PATH = conf.get("hdfs_didi_upload");
        HDFS_URL = conf.get("hdfs_url");

        conf.set("fs.defaultFS", HDFS_URL);
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.job.reduces", "20");
        conf.set("dfs.support.append", "true");
        conf.set("mapreduce.map.memory.mb", "5120");
        conf.set("mapreduce.reduce.memory.mb", "5120");
        conf.set("mapreduce.admin.map.child.java.opts", "-Xmx4096m");
        conf.set("mapreduce.job.priority", "HIGH");
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "128000000");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "128000000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("mapreduce.output.fileoutputformat.compress.codec",
                "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("io.compression.codecs",
                "com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec");


        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void upload2Hdfs(File localpath, String dpath) throws IllegalArgumentException,
            IOException {
        InputStream sin = new FileInputStream(localpath);
        OutputStream out = hdfs.create(new Path(dpath));
        IOUtils.copyBytes(sin, out, 10 * 1024 * 1024, true);

    }

    public static Path gzCompressFile(String path) throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Path p = new Path(path + codec.getDefaultExtension());
        try {
            InputStream in = hdfs.open(new Path(path));
            OutputStream out = codec.createOutputStream(hdfs.create(p));
            IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return p;
    }

    public static void copyBytes(InputStream in, OutputStream out, int buffSize) throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException {
        try {
            copyBytes(in, out, buffSize);
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }
        } finally {
            if (close) {
                IOUtils.closeStream(out);
                IOUtils.closeStream(in);
            }
        }
    }


    public static Path snappyCompressFile(String path) throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Path p = new Path(path + codec.getDefaultExtension());
        try {
            InputStream in = hdfs.open(new Path(path));
            OutputStream out = new SnappyOutputStream(hdfs.create(p));
            IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return p;
    }

    public static Path snappyUnCompressFile(String path) throws Exception {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        Path p = new Path(path + codec.getDefaultExtension());
        try {
            InputStream in = new SnappyInputStream(hdfs.open(new Path(path)));
            OutputStream out = hdfs.create(new Path(path.replaceAll(".snappy", "")));
            IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return p;
    }

    private static void lzoUncompressFile(String path) throws Exception {
        CompressionCodec lzo = ReflectionUtils.newInstance(LzopCodec.class, conf);
        Path p = new Path(path + lzo.getDefaultExtension());
        InputStream in = lzo.createInputStream(hdfs.open(new Path(path)));
        OutputStream out = hdfs.create(new Path(path.replaceAll(".lzo", "")));
        IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        LzoIndex.createIndex(hdfs, p);
    }


    private static Path lzocompressFile(String path) throws Exception {
        CompressionCodec lzo = ReflectionUtils.newInstance(LzopCodec.class, conf);
        Path p = new Path(path + lzo.getDefaultExtension());
        InputStream in = hdfs.open(new Path(path));
        OutputStream out = lzo.createOutputStream(hdfs.create(p));
        IOUtils.copyBytes(in, out, 10 * 1024 * 1024, true);
        LzoIndex.createIndex(hdfs, p);
        return p;
    }

    public static void lzoIndexSFile(Path path) throws IOException {
        LzoIndexer index = new LzoIndexer(conf);
        index.index(path);
    }

    public Set<FileStatus> iteratorShowFiles(FileSystem hdfs, org.apache.hadoop.fs.Path path,
            PathFilter filter, Set<FileStatus> list) {
        try {
            if (hdfs == null || path == null) {
                return null;
            }
            // 获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            // 展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory()) {
                        iteratorShowFiles(hdfs, files[i].getPath(), filter, list);
                    } else if (files[i].isFile() && filter.accept(files[i].getPath())) {
                        list.add(files[i]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public void startCheckDirThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                final Set<FileStatus> files = new HashSet<FileStatus>();
                while (true) {
                    lastSet.addAll(files);
                    iteratorShowFiles(hdfs, new org.apache.hadoop.fs.Path(TEMP_HDFS_PARENT_PATH),
                            new PathFilter() {
                                @Override
                                public boolean accept(org.apache.hadoop.fs.Path path) {
                                    return path != null && !path.getName().contains("_ing")
                                            && !path.getName().contains("gz")
                                            && !path.getName().contains("snappy")
                                            && !path.getName().contains("lzo");
                                    // && !path.getName().contains("index")
                                    // && path.toString().contains("20170825");
                                }
                            }, files);
                    files.removeAll(lastSet);
                    compressQueue.addAll(files);
                }
            }

        }).start();
    }

    public static void main(String[] args) throws InterruptedException {
        DidiUncompress didi = new DidiUncompress();
        didi.startCheckDirThread();
        while (true) {
            System.out.println(didi.compressQueue.take());
        }
        // while (true) {
        // try {
        // final FileStatus take = didi.compressQueue.take();
        // System.out.println(take.getPath());
        // Callable<Boolean> call = new Callable<Boolean>() {
        // public Boolean call() throws Exception {
        // System.out.println(take);
        // Path p = lzocompressFile(take.getPath().toString());
        // lzoIndexSFile(p);
        // boolean delete = hdfs.delete(take.getPath(), false);
        // log.info(take.getPath() + " 文件删除 " + (delete ? "成功" : "失败"));
        // Path lzo = new Path(take.getPath() + ".lzo");
        // FileStatus lzofileStatus = hdfs.listStatus(lzo)[0];
        // System.out.println(lzofileStatus);
        // LogEntity datahiveBean = new LogEntity();
        // datahiveBean.setTable("datalist");
        // LogEntity.JsonStr jsonStr = datahiveBean.new JsonStr();
        // jsonStr.setCoord("gcj02");
        // jsonStr.setAdmincode(lzo.getParent().getName());
        // jsonStr.setPath(lzofileStatus.getPath().toString()
        // .replaceAll("hdfs://master", ""));
        // jsonStr.setVersion(lzofileStatus.getPath().getName()
        // .replaceAll("\\.lzo", ""));
        // jsonStr.setData_type("didiTrajectory");
        // jsonStr.setDesc("didi");
        // jsonStr.setSize(String.valueOf(lzofileStatus.getLen()));
        // jsonStr.setSplits(",");
        // datahiveBean.setContent((JSONObject) JSON.toJSON(jsonStr));
        // String jsonParam = JSON.toJSONString(datahiveBean).replaceAll("\\\\", "");
        // jsonParam = java.net.URLEncoder.encode(jsonParam);
        // String url =
        // "http://172.16.10.142:8080/log-collector/log?json=" + jsonParam;
        // String doGet = HttpClientUtil.doGet(url);
        // System.out.println(doGet);
        // return false;
        // }
        // };
        // Future<Boolean> submit = didi.threadPool.submit(call);
        // submit.get();
        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        // }

    }
}
