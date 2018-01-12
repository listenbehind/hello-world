package com.minedata.service.rticHistory;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by panzongqi on 2017/4/5.
 */
public class FormatCrspd {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public ConcurrentHashMap<Long, List<LinkInfo>> getValue(String crspdPath) {
        ConcurrentHashMap<Long, List<LinkInfo>> rtic2Links =
                new ConcurrentHashMap<Long, List<LinkInfo>>();

        File crspFile = new File(crspdPath);

        File[] crspList = crspFile.listFiles();

        for (File file : crspList) {
            if (file.isDirectory()) {
                continue;
            }
            String fileName = file.getName();
            if (!fileName.contains("correspondingOf") || !fileName.endsWith(".csv")) {
                continue;
            }

            formatCrspd(file, rtic2Links);
        }

        return rtic2Links;
    }

    private void formatCrspd(File file, Map<Long, List<LinkInfo>> rtic2Links) {

        BufferedReader bf = null;

        try {
            String data;
            bf = new BufferedReader(new FileReader(file));
            bf.readLine();
            while ((data = bf.readLine()) != null) {
                String[] datas = data.split(",");
                int kind = Integer.parseInt(datas[1]);
                long rticid = Long.parseLong(datas[2]) + kind * 10000;
                long meshid = Long.parseLong(datas[0]);
                /**
                 * 保存RTIC中包含的有序的NILINK及相关信息
                 */
                rtic2Links.putIfAbsent(rticid, new ArrayList<LinkInfo>());
                int sLen = 0;
                for (int i = 6; i < datas.length; i += 2) {
                    int len = Integer.parseInt(datas[i + 1]);
                    long linkId = Long.parseLong(datas[i]);

                    /**
                     * 此处不考虑精度问题，统一由后续方法进行弥补
                     */
                    int startLen = (sLen /* + 5 */) / 10 * 10;

                    LinkInfo linkInfo = new LinkInfo(startLen, len, linkId, meshid);

                    rtic2Links.get(rticid).add(linkInfo);

                    sLen += len;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            if (bf != null) {
                try {
                    bf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("File closed error, " + e.getMessage());
                }
            }
        }
    }
}
