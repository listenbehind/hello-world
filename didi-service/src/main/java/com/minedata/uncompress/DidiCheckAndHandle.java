package com.minedata.uncompress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;

import com.alibaba.fastjson.JSON;
import com.minedata.didi.DirCheckBean;
import com.minedata.utils.HttpClientUtil;
import com.minedata.utils.QuartzUtil;



public class DidiCheckAndHandle implements Job {

    private static final Logger log = Logger.getLogger(DidiCheckAndHandle.class.getName());

    public static boolean tarUnPackLinux(File file) {
        String[] tarCmds = {"tar", "xvf", file.getAbsolutePath(), "-C", file.getParent()};
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


    // public static void main(String[] args) {
    // String parseObject =
    // "{\"version\":\"" + args[0] + "\",\"region\":\"" + "120000"
    // + "\",\"path\":\"/databus/didi/" + args[0] + "\"}";
    // String response =
    // HttpClientUtil.doPost("http://10.20.20.24:8085/didiOperation/check", parseObject);
    //
    // DirCheckBean checkBean = JSON.parseObject(response, DirCheckBean.class);
    // String version = checkBean.getVersion();
    // Map<String, String> wrongTar = checkBean.getWrongTar();
    // Map<String, String> missingDir = checkBean.getMissingDir();
    // List<String> dirList = checkBean.getDirList();
    //
    // if (null != dirList && dirList.size() != 0 && wrongTar.size() == 0) {
    // for (String region : dirList) {
    //
    // /** ftpFile **/
    // File ftpfile =
    // new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
    // + File.separator + region + File.separator + "gcj02-ftp"
    // + File.separator + region + "-ftp" + ".tar");
    // if (ftpfile.exists()) {
    // boolean isFtpTarFinish = tarUnPackLinux(ftpfile);
    // System.out.println(ftpfile + "---tar---" + isFtpTarFinish);
    // if (!isFtpTarFinish) {
    // log.error("[" + ftpfile + "] tar uncompress fail");
    // continue;
    // }
    // ftpfile.delete();
    // }
    //
    // File ftp_gzFile =
    // new File(ftpfile.getParent() + File.separator + "data/didi_city/didiData"
    // + File.separator + version + File.separator + region);
    // System.out.println("gz path" + ftp_gzFile);
    // File[] ftp_listgzFiles = ftp_gzFile.listFiles(new FilenameFilter() {
    // @Override
    // public boolean accept(File dir, String name) {
    // return name.contains("gz");
    // }
    //
    // });
    // if (null == ftp_listgzFiles) {
    // System.out.println(ftp_gzFile + " not exist");
    // continue;
    // }
    // for (File ftp_gzfile : ftp_listgzFiles) {
    // String didiStruct = ftpfile.getParent().replaceAll("/APP/databus/didi/", "");
    // System.out.println("----------gz" + ftp_gzfile);
    // System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
    // + File.separator + didiStruct);
    // try {
    // DidiUncompress.upload2Hdfs(ftp_gzfile,
    // DidiUncompress.ORGIN_FILE_PARENT_PATH + File.separator + didiStruct
    // + File.separator + ftp_gzfile.getName());
    // System.out.println("---------hdfs uploaded" + ftp_gzfile);
    // } catch (IllegalArgumentException e) {
    // e.printStackTrace();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }
    //
    // /** compressionFile **/
    // File compressionfile =
    // new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
    // + File.separator + region + File.separator + "gcj02-compression"
    // + File.separator + region + "-compression" + ".tar");
    // if (!ftpfile.exists()) {
    // boolean isCompressionTarFinish = tarUnPackLinux(compressionfile);
    // System.out.println(compressionfile + "---tar---" + isCompressionTarFinish);
    // if (!isCompressionTarFinish) {
    // log.error("[" + compressionfile + "] tar uncompress fail");
    // continue;
    // }
    // ftpfile.delete();
    // }
    //
    // File com_gzFile =
    // new File(compressionfile.getParent() + File.separator
    // + "data/didi_city/didiData" + File.separator + version
    // + File.separator + region);
    // System.out.println("gz path" + com_gzFile);
    // File[] com_listgzFiles = com_gzFile.listFiles(new FilenameFilter() {
    // @Override
    // public boolean accept(File dir, String name) {
    // return name.contains("gz");
    // }
    //
    // });
    // if (null == com_listgzFiles) {
    // System.out.println(com_gzFile + " not exist");
    // continue;
    // }
    // for (File com_gzfile : com_listgzFiles) {
    // String didiStruct =
    // compressionfile.getParent().replaceAll("/APP/databus/didi/", "");
    // System.out.println("----------gz" + com_gzfile);
    // System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
    // + File.separator + didiStruct);
    // try {
    // DidiUncompress.upload2Hdfs(com_gzfile,
    // DidiUncompress.ORGIN_FILE_PARENT_PATH + File.separator + didiStruct
    // + File.separator + com_gzfile.getName());
    // System.out.println("---------hdfs uploaded" + com_gzfile);
    // } catch (IllegalArgumentException e) {
    // e.printStackTrace();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    //
    // }
    //
    // if (missingDir.size() != 0) {
    // Set<Entry<String, String>> entrySet = missingDir.entrySet();
    // for (Entry<String, String> entry : entrySet) {
    // String childDir = entry.getValue();
    // String flag;
    // if (childDir.contains("compression")) {
    // childDir = childDir.replaceAll("compression", "ftp");
    // flag = "ftp";
    // } else {
    // childDir = childDir.replaceAll("ftp", "compression");
    // flag = "compression";
    // }
    // String key = entry.getKey();
    // File file =
    // new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
    // + File.separator + key + File.separator + childDir + File.separator
    // + key + "-" + flag + ".tar");
    // if (file.exists()) {
    // boolean isTarFinish = tarUnPackLinux(file);
    // System.out.println(file + "---tar---" + isTarFinish);
    // if (!isTarFinish) {
    // log.error("[" + file + "] tar uncompress fail");
    // continue;
    // }
    // }
    //
    // File gzFile =
    // new File(file.getParent() + File.separator + "data/didi_city/didiData"
    // + File.separator + version + File.separator + key);
    // System.out.println("gz path" + gzFile);
    // File[] listgzFiles = gzFile.listFiles(new FilenameFilter() {
    // @Override
    // public boolean accept(File dir, String name) {
    // return name.contains("gz");
    // }
    //
    // });
    // if (null == listgzFiles) {
    // System.out.println(gzFile + " not exist");
    // continue;
    // }
    // for (File gzfile : listgzFiles) {
    // String didiStruct = file.getParent().replaceAll("/APP/databus/didi/", "");
    // System.out.println("----------gz" + gzfile);
    // System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
    // + File.separator + didiStruct);
    // try {
    // DidiUncompress.upload2Hdfs(gzfile, DidiUncompress.ORGIN_FILE_PARENT_PATH
    // + File.separator + didiStruct + File.separator + gzfile.getName());
    // System.out.println("---------hdfs uploaded" + gzfile);
    // } catch (IllegalArgumentException e) {
    // e.printStackTrace();
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }
    // }
    // }
    // }

    public static void main(String[] args) {
        DidiCheckAndHandle job = new DidiCheckAndHandle();
        String quartzTime = "0 10 0 * * ?"; //
        try {
            QuartzUtil.addJob("didi_transfer", job, quartzTime);
        } catch (SchedulerException e) {
            log.error(e.getMessage(), e);
        } catch (ParseException e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(System.currentTimeMillis()));
        calendar.add(Calendar.DATE, -2);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String daybeforeYesterday = sdf.format(calendar.getTime());
        String parseObject =
                "{\"version\":\"" + daybeforeYesterday + "\",\"region\":\"" + "120000"
                        + "\",\"path\":\"/databus/didi/" + daybeforeYesterday + "\"}";
        String response =
                HttpClientUtil.doPost("http://10.20.20.24:8085/didiOperation/check", parseObject);


        DirCheckBean checkBean = JSON.parseObject(response, DirCheckBean.class);
        String version = checkBean.getVersion();
        Map<String, String> wrongTar = checkBean.getWrongTar();
        Map<String, String> missingDir = checkBean.getMissingDir();
        List<String> dirList = checkBean.getDirList();

        if (null != dirList && dirList.size() != 0 && wrongTar.size() == 0) {
            for (String region : dirList) {

                /** ftpFile **/
                File ftpfile =
                        new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
                                + File.separator + region + File.separator + "gcj02-ftp"
                                + File.separator + region + "-ftp" + ".tar");
                if (ftpfile.exists()) {
                    boolean isFtpTarFinish = tarUnPackLinux(ftpfile);
                    System.out.println(ftpfile + "---tar---" + isFtpTarFinish);
                    if (!isFtpTarFinish) {
                        log.error("[" + ftpfile + "] tar uncompress fail");
                        continue;
                    }
                    ftpfile.delete();
                }

                File ftp_gzFile =
                        new File(ftpfile.getParent() + File.separator + "data/didi_city/didiData"
                                + File.separator + version + File.separator + region);
                System.out.println("gz path" + ftp_gzFile);
                File[] ftp_listgzFiles = ftp_gzFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains("gz");
                    }

                });
                if (null == ftp_listgzFiles) {
                    System.out.println(ftp_gzFile + " not exist");
                    continue;
                }
                for (File ftp_gzfile : ftp_listgzFiles) {
                    String didiStruct = ftpfile.getParent().replaceAll("/APP/databus/didi/", "");
                    System.out.println("----------gz" + ftp_gzfile);
                    System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
                            + File.separator + didiStruct);
                    try {
                        DidiUncompress.upload2Hdfs(ftp_gzfile,
                                DidiUncompress.ORGIN_FILE_PARENT_PATH + File.separator + didiStruct
                                        + File.separator + ftp_gzfile.getName());
                        System.out.println("---------hdfs uploaded" + ftp_gzfile);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                /** compressionFile **/
                File compressionfile =
                        new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
                                + File.separator + region + File.separator + "gcj02-compression"
                                + File.separator + region + "-compression" + ".tar");
                if (!ftpfile.exists()) {
                    boolean isCompressionTarFinish = tarUnPackLinux(compressionfile);
                    System.out.println(compressionfile + "---tar---" + isCompressionTarFinish);
                    if (!isCompressionTarFinish) {
                        log.error("[" + compressionfile + "] tar uncompress fail");
                        continue;
                    }
                    ftpfile.delete();
                }

                File com_gzFile =
                        new File(compressionfile.getParent() + File.separator
                                + "data/didi_city/didiData" + File.separator + version
                                + File.separator + region);
                System.out.println("gz path" + com_gzFile);
                File[] com_listgzFiles = com_gzFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains("gz");
                    }

                });
                if (null == com_listgzFiles) {
                    System.out.println(com_gzFile + " not exist");
                    continue;
                }
                for (File com_gzfile : com_listgzFiles) {
                    String didiStruct =
                            compressionfile.getParent().replaceAll("/APP/databus/didi/", "");
                    System.out.println("----------gz" + com_gzfile);
                    System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
                            + File.separator + didiStruct);
                    try {
                        DidiUncompress.upload2Hdfs(com_gzfile,
                                DidiUncompress.ORGIN_FILE_PARENT_PATH + File.separator + didiStruct
                                        + File.separator + com_gzfile.getName());
                        System.out.println("---------hdfs uploaded" + com_gzfile);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }

        if (missingDir.size() != 0) {
            Set<Entry<String, String>> entrySet = missingDir.entrySet();
            for (Entry<String, String> entry : entrySet) {
                String childDir = entry.getValue();
                String flag;
                if (childDir.contains("compression")) {
                    childDir = childDir.replaceAll("compression", "ftp");
                    flag = "ftp";
                } else {
                    childDir = childDir.replaceAll("ftp", "compression");
                    flag = "compression";
                }
                String key = entry.getKey();
                File file =
                        new File(DidiUncompress.FILE_PARENT_PATH + File.separator + version
                                + File.separator + key + File.separator + childDir + File.separator
                                + key + "-" + flag + ".tar");
                if (file.exists()) {
                    boolean isTarFinish = tarUnPackLinux(file);
                    System.out.println(file + "---tar---" + isTarFinish);
                    if (!isTarFinish) {
                        log.error("[" + file + "] tar uncompress fail");
                        continue;
                    }
                }

                File gzFile =
                        new File(file.getParent() + File.separator + "data/didi_city/didiData"
                                + File.separator + version + File.separator + key);
                System.out.println("gz path" + gzFile);
                File[] listgzFiles = gzFile.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.contains("gz");
                    }

                });
                if (null == listgzFiles) {
                    System.out.println(gzFile + " not exist");
                    continue;
                }
                for (File gzfile : listgzFiles) {
                    String didiStruct = file.getParent().replaceAll("/APP/databus/didi/", "");
                    System.out.println("----------gz" + gzfile);
                    System.out.println("----------" + DidiUncompress.ORGIN_FILE_PARENT_PATH
                            + File.separator + didiStruct);
                    try {
                        DidiUncompress.upload2Hdfs(gzfile, DidiUncompress.ORGIN_FILE_PARENT_PATH
                                + File.separator + didiStruct + File.separator + gzfile.getName());
                        System.out.println("---------hdfs uploaded" + gzfile);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        calendar.add(Calendar.DATE, -1);
        String uncompressDay = sdf.format(calendar.getTime());
        String doGet =
                HttpClientUtil.doGet("http://10.20.20.24:8085/didiOperation/uncompress/"
                        + uncompressDay);
    }
}
