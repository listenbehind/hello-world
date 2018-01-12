package com.minedata.service.didi;


import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import com.alibaba.dubbo.rpc.protocol.rest.support.ContentType;
import com.minedata.didi.DidiEntity;
import com.minedata.didi.DidiRestService;
import com.minedata.didi.DirCheckBean;
import com.minedata.service.didi.store.Store;
import com.minedata.service.didi.store.impl.HdfsStore;
import com.netflix.config.DynamicPropertyFactory;

@Path("didiOperation")
@Service("didiRestService")
@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
@Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_XML_UTF_8})
public class DidiRestServiceImpl implements DidiRestService {

    private final static Logger log = Logger.getLogger(DidiRestServiceImpl.class);
    private static FileSystem hdfs = null;
    private static final DynamicPropertyFactory configInstance =
            com.netflix.config.DynamicPropertyFactory.getInstance();
    private ExecutorService threadPool = Executors.newCachedThreadPool();

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
                DidiRestServiceImpl.class.getClassLoader().getResourceAsStream("./env_config.xml"),
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

        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<FileStatus> iteratorShowFiles(FileSystem hdfs, org.apache.hadoop.fs.Path path,
            PathFilter filter, List<FileStatus> list, String version, String region) {
        try {
            if (hdfs == null || path == null) {
                return null;
            }
            // 获取文件列表
            FileStatus[] files = hdfs.listStatus(path);
            // 展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory() && files[i].getPath().toString().contains(region)) {
                        System.out.println(files[i].getPath());
                        iteratorShowFiles(hdfs, files[i].getPath(), filter, list, version, region);
                    } else if (files[i].isFile() && filter.accept(files[i].getPath())) {
                        System.out.println("[Path: ]" + files[i].getPath());
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

    public Map<String, List<FileStatus>> iteratorShowFiles(FileSystem hdfs,
            org.apache.hadoop.fs.Path path, PathFilter filter, Map<String, List<FileStatus>> map) {
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
                        System.out.println("[uncompress dir]:" + files[i].getPath());
                        iteratorShowFiles(hdfs, files[i].getPath(), filter, map);
                    } else if (files[i].isFile() && filter.accept(files[i].getPath())) {
                        System.out.println("[uncompress dir Path: ]" + files[i].getPath());
                        // list.add(files[i]);
                        String key = files[i].getPath().getParent().getParent().getName();
                        if (map.containsKey(key)) {
                            List<FileStatus> list = map.get(key);
                            list.add(files[i]);
                        } else {
                            List<FileStatus> list = new ArrayList<FileStatus>();
                            list.add(files[i]);
                            map.put(key, list);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static boolean tarUnPackLinux(File file) {
        String[] tarCmds = {"tar", "tvf", file.getAbsolutePath()};
        boolean isTarDone = true;
        try {
            Process tarPro = Runtime.getRuntime().exec(tarCmds);
            try {
                InputStreamReader isr = new InputStreamReader(tarPro.getErrorStream(), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null && isTarDone) {
                    if (!line.contains("Error") && !line.contains("Skipping")) {
                    } else {
                        log.info(line);
                        isTarDone = false;
                    }
                }
            } catch (IOException ioe) {
                log.error(ioe.getMessage(), ioe);
                isTarDone = false;
            }
            if (isTarDone) {
                tarPro.destroy();
                tarPro.waitFor();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return isTarDone;
    }

    public DirCheckBean checkLocalDir(DirCheckBean bean) {

        String version = bean.getVersion();
        String region = bean.getRegion();
        Map<String, String> missingDir = bean.getMissingDir();// 未上传到hdfs的目录
        List<String> dirList = bean.getDirList();// 本地所有的城市列表
        Map<String, String> wrongTar = new HashMap<String, String>();// 发错目录的tar包
        if (missingDir.size() != 0) {
            Set<Entry<String, String>> entrySet = missingDir.entrySet();
            for (Entry<String, String> entry : entrySet) {
                String childDir = entry.getValue();
                if (childDir.contains("compression")) {
                    childDir = childDir.replaceAll("compression", "ftp");
                } else {
                    childDir = childDir.replaceAll("ftp", "compression");
                }
                String key = entry.getKey();
                File file =
                        new File(FILE_PARENT_PATH + File.separator + version + File.separator + key
                                + File.separator + childDir);
                System.out.println("---path---" + file);
                if (!file.exists()) {
                    wrongTar.put(key, file.getAbsolutePath());
                    continue;
                }
                File[] listFiles = file.listFiles(new FilenameFilter() {

                    @Override
                    public boolean accept(File dir, String name) {
                        // TODO Auto-generated method stub
                        return name.contains("tar");
                    }
                });
                for (File tarFile : listFiles) {
                    System.out.println("---tar---" + tarFile);
                    if (!tarFile.getName().contains(key)) {
                        wrongTar.put(key, tarFile.getAbsolutePath());
                    } else if (!tarUnPackLinux(tarFile)) {
                        wrongTar.put(key, tarFile.getAbsolutePath());
                    }
                }
            }
        }

        for (String key : dirList) {
            File file_ftp =
                    new File(FILE_PARENT_PATH + File.separator + version + File.separator + key
                            + File.separator + "gcj02-ftp");
            File file_compression =
                    new File(FILE_PARENT_PATH + File.separator + version + File.separator + key
                            + File.separator + "gcj02-compression");

            File[] listFiles_ftp = file_ftp.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    // TODO Auto-generated method stub
                    return name.contains("tar");
                }
            });
            for (File tarFile : listFiles_ftp) {
                System.out.println("---tar---" + tarFile);
                if (!tarFile.getName().contains(key)) {
                    wrongTar.put(key, tarFile.getAbsolutePath());
                } else if (!tarUnPackLinux(tarFile)) {
                    wrongTar.put(key, tarFile.getAbsolutePath());
                }
            }
            File[] listFiles_compression = file_compression.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    // TODO Auto-generated method stub
                    return name.contains("tar");
                }
            });
            for (File tarFile : listFiles_compression) {
                System.out.println("---tar---" + tarFile);
                if (!tarFile.getName().contains(key)) {
                    wrongTar.put(key, tarFile.getAbsolutePath());
                } else if (!tarUnPackLinux(tarFile)) {
                    wrongTar.put(key, tarFile.getAbsolutePath());
                }
            }
        }
        bean.setWrongTar(wrongTar);
        return bean;

    }

    public DirCheckBean getLocalFileList(String path) {
        File localFile = new File(path);
        File[] listFiles = localFile.listFiles();
        List<String> listFileName = new ArrayList<String>();
        for (File file : listFiles) {
            listFileName.add(file.getName());
        }
        DirCheckBean bean = new DirCheckBean();
        bean.setDirList(listFileName);
        bean.setSize(listFiles.length);
        return bean;

    }

    public boolean checkHdfsDir(FileSystem hdfs, org.apache.hadoop.fs.Path path,
            DirCheckBean dirCheckBean) {
        List<String> dirList = dirCheckBean.getDirList();
        Map<String, String> missMap = new HashMap<String, String>();
        boolean isfinish = true;
        if (hdfs == null || path == null) {
            return false;
        }
        try {
            FileStatus[] files = hdfs.listStatus(path);
            for (FileStatus file : files) {
                dirList.remove(file.getPath().getName());
                FileStatus[] listStatus = hdfs.listStatus(file.getPath());
                if (listStatus.length != 2) {
                    missMap.put(file.getPath().getName(), listStatus[0].getPath().getName());
                    isfinish = false;
                }
            }
            dirCheckBean.setMissingDir(missMap);
            dirCheckBean.setMissingDirParentList(missMap.keySet());

        } catch (IOException e) {
            e.printStackTrace();
            if (e.getMessage().contains("does not exist")) {
                System.out.println("error file does not exist");
                dirCheckBean.setDirList(dirList);
                dirCheckBean.setMissingDir(missMap);
                dirCheckBean.setMissingDirParentList(missMap.keySet());
            }
            return false;
        }
        return isfinish;

    }

    @GET
    @Path("uncompress/{version}/{region}")
    @Async
    @Override
    public Future<String> uncompressFileByParams(@PathParam("version") String version,
            @PathParam("region") String region) {
        String s = ORGIN_FILE_PARENT_PATH + File.separator + version + File.separator + region;
        System.out.println("------------path: " + s);
        // String version = didi.getVersion();
        // String region = didi.getRegion();
        final DidiEntity returnBody = new DidiEntity(version, region, "");
        final List<FileStatus> files = new ArrayList<FileStatus>();
        // FileStatus[] files;
        try {
            // files = hdfs.listStatus(new org.apache.hadoop.fs.Path(s), new PathFilter() {
            //
            // @Override
            // public boolean accept(org.apache.hadoop.fs.Path path) {
            // // hdfs.exists(new org.apache.hadoop.fs.Path(path.getParent(), path
            // // .getName() + MD5_SUFFIX))
            // // &&
            // return path != null && !path.getName().contains("gzing")
            // && path.getName().contains("gz");
            // }
            // });
            iteratorShowFiles(hdfs, new org.apache.hadoop.fs.Path(s), new PathFilter() {

                @Override
                public boolean accept(org.apache.hadoop.fs.Path path) {
                    return path != null && !path.getName().contains("gzing")
                            && path.getName().contains("gz");
                }
            }, files, version, region);
            Store store = new HdfsStore(conf);
            for (FileStatus s1 : files) {
                log.info(s1.getPath());
                org.apache.hadoop.fs.Path absolutePath = s1.getPath();
                store.storeHDFS(absolutePath, returnBody);

            }
            log.info("拷贝完成，写入" + FINISH);
            boolean finish = store.storeHDFS(new org.apache.hadoop.fs.Path(FINISH), returnBody);

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new AsyncResult<String>(returnBody.getPath());
    }

    @GET
    @Path("uncompress/{version}")
    @Async
    @Override
    public Future<String> uncompressFile(@PathParam("version") String version) {
        String s = ORGIN_FILE_PARENT_PATH + File.separator + version;
        System.out.println("------------path: " + s);
        final DidiEntity returnBody = new DidiEntity(version, "", "");
        final Map<String, List<FileStatus>> files = new HashMap<String, List<FileStatus>>();
        // FileStatus[] files;
        try {
            // files = hdfs.listStatus(new org.apache.hadoop.fs.Path(s), new PathFilter() {
            //
            // @Override
            // public boolean accept(org.apache.hadoop.fs.Path path) {
            // // hdfs.exists(new org.apache.hadoop.fs.Path(path.getParent(), path
            // // .getName() + MD5_SUFFIX))
            // // &&
            // return path != null && !path.getName().contains("gzing")
            // && path.getName().contains("gz");
            // }
            // });
            iteratorShowFiles(hdfs, new org.apache.hadoop.fs.Path(s), new PathFilter() {

                @Override
                public boolean accept(org.apache.hadoop.fs.Path path) {
                    return path != null && !path.getName().contains("gzing")
                            && path.getName().contains("gz");
                }
            }, files);
            Store store = new HdfsStore(conf);
            Set<Entry<String, List<FileStatus>>> entrySet = files.entrySet();
            // for (FileStatus s1 : files) {
            // log.info(s1.getPath());
            // org.apache.hadoop.fs.Path absolutePath = s1.getPath();
            // store.storeHDFS(absolutePath, returnBody);
            //
            // }
            for (Entry<String, List<FileStatus>> entry : entrySet) {
                List<FileStatus> value = entry.getValue();
                returnBody.setRegion(entry.getKey());
                for (FileStatus s1 : value) {
                    log.info(s1.getPath());
                    org.apache.hadoop.fs.Path absolutePath = s1.getPath();
                    store.storeHDFS(absolutePath, returnBody);
                }
                log.info("拷贝完成，写入" + FINISH);
                boolean finish = store.storeHDFS(new org.apache.hadoop.fs.Path(FINISH), returnBody);
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new AsyncResult<String>(returnBody.getPath());
    }

    @POST
    @Path("/check")
    @Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_XML_UTF_8})
    @Consumes(ContentType.APPLICATION_JSON_UTF_8)
    @Override
    public DirCheckBean ping(DidiEntity didi) {
        System.out.println("------------path: " + didi.getPath());
        String s = didi.getPath();
        String version = didi.getVersion();
        String region = didi.getRegion();
        String hdfsPath = new String(s);
        s = "/APP" + s;
        didi.setPath(s);
        DirCheckBean dirCheckBean = getLocalFileList(s);
        dirCheckBean.setRegion(region);
        dirCheckBean.setVersion(version);
        checkHdfsDir(hdfs, new org.apache.hadoop.fs.Path(hdfsPath), dirCheckBean);
        // checkLocalDir(dirCheckBean);
        return dirCheckBean;

    }

    @POST
    @Path("/listFile")
    @Produces({ContentType.APPLICATION_JSON_UTF_8, ContentType.TEXT_XML_UTF_8})
    @Consumes(ContentType.APPLICATION_JSON_UTF_8)
    @Override
    public DirCheckBean list(DidiEntity didi) {
        String s = didi.getPath();
        return getLocalFileList(s);

    }

}
