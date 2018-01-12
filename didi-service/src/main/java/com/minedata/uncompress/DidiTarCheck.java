package com.minedata.uncompress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DidiTarCheck {
    public static boolean tarUnPackLinux(File file) {
        String[] tarCmds = {"tar", "tvf", file.getAbsolutePath(), "-C", file.getParent()};
        boolean isTarDone = true;
        try {
            Process tarPro = Runtime.getRuntime().exec(tarCmds);
            try {
                InputStreamReader isr = new InputStreamReader(tarPro.getInputStream(), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null && isTarDone) {
                    if (!line.contains("Error") && !line.contains("Skipping")) {
                        if (!line.contains("20170823")) {
                            System.out.println(line);
                        } else {
                            System.out.println("pass");
                        }
                    } else {
                        System.out.println(line);
                        isTarDone = false;
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                isTarDone = false;
            }
            if (isTarDone) {
                tarPro.destroy();
                tarPro.waitFor();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isTarDone;
    }

    public static List<File> iteratorShowFiles(File hdfs, String path, FilenameFilter filter,
            List<File> list) {
        try {
            if (hdfs == null || path == null) {
                return null;
            }
            // 获取文件列表
            File[] files = hdfs.listFiles();
            // 展示文件信息
            for (int i = 0; i < files.length; i++) {
                try {
                    if (files[i].isDirectory()) {
                        File[] listFiles = files[i].listFiles();
                        for (File listfile : listFiles) {
                            if (listfile.isFile() && filter.accept(listfile, files[i].getPath())) {
                                list.add(listfile);
                            } else if (listfile.isDirectory()) {
                                File[] listFiles2 = listfile.listFiles();
                                for (File listfile2 : listFiles2) {
                                    if (listfile2.isFile() && listfile2.getName().contains("tar")) {
                                        list.add(listfile2);
                                    }
                                }
                            }
                        }
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

    public static void main(String[] args) {
        File file = new File(args[0]);
        List<File> list = new ArrayList<File>();
        iteratorShowFiles(file, file.getAbsolutePath(), new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("tar");
            }

        }, list);
        for (File tar : list) {
            tarUnPackLinux(tar);
        }
    }
}
