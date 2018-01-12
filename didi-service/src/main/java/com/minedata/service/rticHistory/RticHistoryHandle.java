package com.minedata.service.rticHistory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RticHistoryHandle {

    public static File lzoUncompressFile(String lzopPath, String input, String output) {
        File outfile = new File(output);

        if (!outfile.getParentFile().exists()) {
            outfile.getParentFile().mkdir();
        }
        File infile = new File(input);
        if (!infile.exists()) {
            return null;
        }
        String strCmd = lzopPath + File.separator + "lzop -d " + input + " -o " + output;
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(strCmd);
            int waitFor = process.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new File(output);
    }


    public static void main(String[] args) {
        File inputFiles = new File("E:\\data\\RTIC历史路况\\RTIC\\20171113");
        File[] listFiles = inputFiles.listFiles();
        List<File> outputFiles = new ArrayList<File>();
        for (File file2Decompress : listFiles) {
            System.out.println(file2Decompress.getAbsolutePath());
            outputFiles.add(lzoUncompressFile("E:\\原有数据\\soft\\lzop101w", file2Decompress
                    .getAbsolutePath(), file2Decompress.getParentFile().getAbsolutePath()
                    + "\\output\\" + file2Decompress.getName().replaceAll(".lzo", ".txt")));
        }
    }
}
