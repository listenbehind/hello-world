package com.minedata.uncompress;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import org.apache.log4j.Logger;

import com.minedata.service.didi.DidiRestServiceImpl;
import com.minedata.service.didi.store.DiDiDecompress;
import com.netflix.config.DynamicPropertyFactory;

public class DidiSchedueDownLoad {
    private final static Logger log = Logger.getLogger(DidiRestServiceImpl.class);
    private static FileSystem hdfs = null;
    private static final DynamicPropertyFactory configInstance =
            com.netflix.config.DynamicPropertyFactory.getInstance();
    private ExecutorService threadPool = Executors.newFixedThreadPool(10);
    private LinkedBlockingQueue<File> compressQueue = new LinkedBlockingQueue<File>();
    private Set<File> lastSet = new HashSet<File>();

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
        // try {
        // conf.addResource(new FileInputStream(new File("./env_config.xml")));
        // } catch (FileNotFoundException e1) {
        // e1.printStackTrace();
        // }


        conf.addResource(
                DidiRestServiceImpl.class.getClassLoader().getResourceAsStream("env_config.xml"),
                "utf-8");
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

    public Set<File> iteratorShowLocalFiles(File file, FilenameFilter filter, Set<File> list) {
        try {
            if (file == null || !file.exists()) {
                return null;
            }
            // 获取文件列表
            File[] files = file.listFiles();
            // 展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory()) {
                        iteratorShowLocalFiles(files[i], filter, list);
                    } else if (files[i].isFile() && filter.accept(files[i], files[i].getName())) {
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

    public void startCheckDirThread(final String version) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                final Set<File> files = new HashSet<File>();
                final File file = new File(FILE_PARENT_PATH);
                while (true) {
                    lastSet.addAll(files);
                    iteratorShowLocalFiles(file, new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return dir != null && dir.getAbsolutePath().contains(version)
                                    && !name.contains("gzing") && name.contains("gz")
                                    && !name.contains("uncompressing") && !name.contains("_ing");
                        }
                    }, files);
                    files.removeAll(lastSet);
                    compressQueue.addAll(files);
                }
            }

        }).start();
    }

    public void download(String version, String region) {
        Set<FileStatus> hdfsFile = new HashSet<FileStatus>();
        try {
            iteratorShowFiles(hdfs, new org.apache.hadoop.fs.Path(ORGIN_FILE_PARENT_PATH
                    + File.separator + version + File.separator + region), new PathFilter() {

                @Override
                public boolean accept(Path path) {
                    return path != null && path.getName().contains("gz");

                }
            }, hdfsFile);
            for (FileStatus filestatus : hdfsFile) {
                File localing =
                        new File(FILE_PARENT_PATH + File.separator + version + File.separator
                                + region + File.separator + filestatus.getPath().getName() + "_ing");
                File local =
                        new File(FILE_PARENT_PATH + File.separator + version + File.separator
                                + region + File.separator + filestatus.getPath().getName());
                if (!local.getParentFile().exists()
                        || !local.getParentFile().getParentFile().exists()) {
                    local.getParentFile().mkdir();
                    local.getParentFile().getParentFile().mkdir();
                }
                OutputStream outputStream = new FileOutputStream(localing);
                InputStream inputStream = hdfs.open(filestatus.getPath());
                IOUtils.copyBytes(inputStream, outputStream, 10 * 1024 * 1024, true);
                localing.renameTo(local);
            }
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        final DidiSchedueDownLoad didi = new DidiSchedueDownLoad();
        final String version = args[0];
        final String region = args[1];
        didi.startCheckDirThread(version);
        new Thread(new Runnable() {
            @Override
            public void run() {
                didi.download(version, region);
            }

        }).start();
        while (true) {
            File take = didi.compressQueue.take();
            String outputFile =
                    FILE_PARENT_PATH + File.separator + version + File.separator + region
                            + File.separator + version + "_" + region;
            DiDiDecompress.gz2fileVersion(take.getAbsolutePath(), outputFile);
        }
    }
}
