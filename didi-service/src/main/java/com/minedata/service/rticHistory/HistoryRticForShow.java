package com.minedata.service.rticHistory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.minedata.utils.RedisUtil;



public class HistoryRticForShow {
    public static ConcurrentHashMap<Long, List<LinkInfo>> rtic2Links;


    static {
        initCrspd(RTICProperties.crsPdPath);
    }

    public static void initCrspd(String path) {
        FormatCrspd formatCrspd = new FormatCrspd();
        rtic2Links = formatCrspd.getValue(path);
    }

    public static String getRticID(String meshid, String rticKind, String rticNum) {
        String b1 = "0";
        String b2 = "00";
        String b3 = "000";
        StringBuilder sb = new StringBuilder();
        sb.append(meshid).append(rticKind);
        switch (rticNum.length()) {
            case 1:
                sb.append(b3).append(rticNum);
                break;
            case 2:
                sb.append(b2).append(rticNum);
                break;
            case 3:
                sb.append(b1).append(rticNum);
                break;
            case 4:
                sb.append(rticNum);
                break;
        }
        return sb.toString();
    }

    public static int getCharacterPosition(String string, int num) {
        Matcher slashMatcher = Pattern.compile(",").matcher(string);
        int mIdx = 0;
        while (slashMatcher.find()) {
            mIdx++;
            if (mIdx == num) {
                break;
            }
        }
        return slashMatcher.start();
    }

    public static Set<SectionBean> getSection(String line, int sectionCount, int sumlen) {
        String sectionList = line.substring(getCharacterPosition(line, 10) + 1);
        Set<SectionBean> subList = new HashSet<SectionBean>();
        for (int i = 1; i < sectionCount * 3 - 1; i += 3) {
            SectionBean section = new SectionBean();
            String sectionStr = null;
            if (i != sectionCount * 3 - 2) {
                sectionStr =
                        sectionList.substring(getCharacterPosition(sectionList, i) - 1,
                                getCharacterPosition(sectionList, i + 2));
            } else {
                sectionStr = sectionList.substring(getCharacterPosition(sectionList, i) - 1);
            }
            String[] split = sectionStr.split(",");
            int deg = Integer.parseInt(split[0]);
            int slen = Integer.parseInt(split[1]);
            int dlen = Integer.parseInt(split[2]);
            section.setdLen(dlen);
            section.setsLen(sumlen - slen);
            section.setSubDeg(deg);
            subList.add(section);
        }
        return subList;
    }

    private static void converterRtic2NilinkNoSpeed(int degree, List<LinkInfo> linkList,
            Map<Integer, SectionBean> sLen2SubInfo, long time, Map<String, String> link2Degree,
            int travelTime, int sumlen) {
        int detailDegree = -1;
        int detailLen = 0;
        int degreeLen = 0;

        boolean detailFlag = false;

        int detailCount = 0;


        for (LinkInfo linkInfo : linkList) {
            int sLen = linkInfo.getSlen();
            int len = linkInfo.getLen();
            int linksize = linkList.size();
            int direct = 0;
            long linkid = linkInfo.getLinkId();
            long meshid = linkInfo.getMeshId();
            double speed = 0.0;
            SectionBean sectionBean;
            if (!detailFlag) {
                /**
                 * 因为CNRTIC的保存规格中规定RTIC的长度单位为10米，且在RTIC长度保存的时候直接舍去个位，不进行四舍五入
                 * 在做精细化的时候，虽然长度单位仍然为10米，但是结果进行了四舍五入处理。 因此在通过长度反推link的时候需要考虑10米精度的问题，故有如下处理
                 */
                if ((sectionBean = sLen2SubInfo.get(sLen)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                } else if (sLen != 0 && (sectionBean = sLen2SubInfo.get(sLen + 10)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                } else if (sLen != 0 && (sLen - 10 != 0)
                        && (sectionBean = sLen2SubInfo.get(sLen - 10)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                }
            }
            StringBuilder detailkey = new StringBuilder();
            StringBuilder detailvalue = new StringBuilder();
            StringBuilder key = new StringBuilder();
            StringBuilder value = new StringBuilder();
            double avrageSpeed =
                    new BigDecimal((double) sumlen / (double) travelTime).setScale(2,
                            BigDecimal.ROUND_HALF_UP).doubleValue();

            if (linkid < 0) {
                direct = 1;
            }
            if (detailFlag) {
                /**
                 * 精细化路况处理
                 */
                detailkey.append("detail").append(linkid).append("_").append(direct);
                detailvalue.append(linkid).append(",").append(time).append(",")
                        .append(detailDegree).append(",").append(direct).append(",").append(meshid);
                link2Degree.put(detailkey.toString(), detailvalue.toString());

                /**
                 * 精细化路况长度处理 RTIC精细化处理部分对于路况的长度进行了四舍五入（单位：10米）
                 */
                degreeLen += len;
                int subDLen = (degreeLen + 5) / 10 * 10;
                if (detailLen <= subDLen) {
                    detailFlag = false;
                    degreeLen = 0;
                }
            } else {
                /**
                 * 赋整体路况值
                 */

                detailkey.append("detail").append(linkid).append("_").append(direct);
                detailvalue.append(linkid).append(",").append(time).append(",").append(degree)
                        .append(",").append(direct).append(",").append(meshid);
                link2Degree.put(detailkey.toString(), detailvalue.toString());
            }
            // key.append(linkid).append("_").append(direct);
            // value.append(linkid).append(",").append(time).append(",").append(degree).append(",")
            // .append(direct).append(",").append(meshid);
            // link2Degree.put(key.toString(), value.toString());
        }

    }

    private static void converterRtic2Nilink(int degree, List<LinkInfo> linkList,
            Map<Integer, SectionBean> sLen2SubInfo, long time, Map<String, String> link2Degree,
            int travelTime, int sumlen) {
        int detailDegree = -1;
        int detailLen = 0;
        int degreeLen = 0;

        boolean detailFlag = false;

        int detailCount = 0;


        for (LinkInfo linkInfo : linkList) {
            int sLen = linkInfo.getSlen();
            int len = linkInfo.getLen();
            int linksize = linkList.size();
            int direct = 0;
            long linkid = linkInfo.getLinkId();
            long meshid = linkInfo.getMeshId();
            double speed = 0.0;
            SectionBean sectionBean;
            if (!detailFlag) {
                /**
                 * 因为CNRTIC的保存规格中规定RTIC的长度单位为10米，且在RTIC长度保存的时候直接舍去个位，不进行四舍五入
                 * 在做精细化的时候，虽然长度单位仍然为10米，但是结果进行了四舍五入处理。 因此在通过长度反推link的时候需要考虑10米精度的问题，故有如下处理
                 */
                if ((sectionBean = sLen2SubInfo.get(sLen)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                } else if (sLen != 0 && (sectionBean = sLen2SubInfo.get(sLen + 10)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                } else if (sLen != 0 && (sLen - 10 != 0)
                        && (sectionBean = sLen2SubInfo.get(sLen - 10)) != null) {
                    detailCount++;
                    detailDegree = sectionBean.getSubDeg();
                    detailLen = sectionBean.getdLen();
                    detailFlag = true;
                }
            }
            StringBuilder detailkey = new StringBuilder();
            StringBuilder detailvalue = new StringBuilder();
            StringBuilder key = new StringBuilder();
            StringBuilder value = new StringBuilder();
            double avrageSpeed =
                    new BigDecimal((double) sumlen / (double) travelTime).setScale(2,
                            BigDecimal.ROUND_HALF_UP).doubleValue();

            if (linkid < 0) {
                direct = 1;
            }
            if (detailFlag) {
                /**
                 * 精细化路况处理
                 */
                if (degree == 1) {
                    speed = avrageSpeed;
                } else {
                    switch (detailDegree) {
                        case 1:
                            speed = avrageSpeed * 2;
                            break;
                        case 2:
                            speed = avrageSpeed;
                            break;
                        case 3:
                            speed = avrageSpeed * 0.5;
                            break;
                        case 4:
                            speed = avrageSpeed * 0.1;
                            break;
                        case 0:
                            speed = 0.0;
                            break;
                    }
                }

                // detailkey.append("detail").append(linkid).append("_").append(direct);
                detailkey.append(linkid);
                // detailvalue.append(linkid).append(",").append(time).append(",")
                // .append(detailDegree).append(",").append(direct).append(",").append(meshid)
                // .append(",").append(speed);

                if (link2Degree.containsKey(String.valueOf(linkid))) {
                    String tmp = link2Degree.get(String.valueOf(linkid));
                    double speedtmp = Double.parseDouble(tmp.split(",")[0]);
                    double averageSpeed =
                            new BigDecimal((speedtmp + speed * 3.6) / 2.0).setScale(2,
                                    BigDecimal.ROUND_HALF_UP).doubleValue();
                    detailvalue.append(averageSpeed).append(",").append(detailDegree);
                } else {
                    detailvalue.append(speed * 3.6).append(",").append(detailDegree);

                }
                link2Degree.put(detailkey.toString(), detailvalue.toString());

                /**
                 * 精细化路况长度处理 RTIC精细化处理部分对于路况的长度进行了四舍五入（单位：10米）
                 */
                degreeLen += len;
                int subDLen = (degreeLen + 5) / 10 * 10;
                if (detailLen <= subDLen) {
                    detailFlag = false;
                    degreeLen = 0;
                }
            } else {
                /**
                 * 赋整体路况值
                 */

                // detailkey.append("detail").append(linkid).append("_").append(direct);
                detailkey.append(linkid);
                // detailvalue.append(linkid).append(",").append(time).append(",").append(degree)
                // .append(",").append(direct).append(",").append(meshid).append(",")
                // .append(avrageSpeed);
                if (link2Degree.containsKey(String.valueOf(linkid))) {
                    String tmp = link2Degree.get(String.valueOf(linkid));
                    double speedtmp = Double.parseDouble(tmp.split(",")[0]);
                    double averageSpeedtmp =
                            new BigDecimal((speedtmp + avrageSpeed * 3.6) / 2.0).setScale(2,
                                    BigDecimal.ROUND_HALF_UP).doubleValue();
                    detailvalue.append(averageSpeedtmp).append(",").append(degree);
                } else {
                    detailvalue.append(avrageSpeed * 3.6).append(",").append(degree);

                }
                link2Degree.put(detailkey.toString(), detailvalue.toString());
            }
            // key.append(linkid).append("_").append(direct);
            // value.append(linkid).append(",").append(time).append(",").append(degree).append(",")
            // .append(direct).append(",").append(meshid);
            // link2Degree.put(key.toString(), value.toString());
        }

    }

    public static void save2Redis(File rticFile, RedisUtil redis, String version, String timeSeq) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(rticFile));
            String line;
            Map<String, String> link2Degree = new HashMap<>();
            redis.transaction();
            while ((line = br.readLine()) != null) {
                String[] split = line.split(",");
                long time = Long.parseLong(split[2].trim());
                String meshid = split[3].trim();
                String rticNum = split[4].trim();
                String rticKind = split[6].trim();
                int travelTime = Integer.parseInt(split[7].trim());
                int degree = Integer.parseInt(split[8].trim());
                int sectionCount = Integer.parseInt(split[9].trim());
                String ritcID = getRticID(meshid, rticKind, rticNum);
                List<LinkInfo> linkList = rtic2Links.get(Long.parseLong(ritcID));
                int sumlen = 0;
                if (linkList == null) {
                    continue;
                }
                for (LinkInfo linkInfo : linkList) {
                    int len = linkInfo.getLen();
                    sumlen += len;
                }
                Map<Integer, SectionBean> sLen2SubInfo = new HashMap<>();
                if (sectionCount > 0) {
                    Set<SectionBean> subList = getSection(line, sectionCount, sumlen);
                    for (SectionBean sectionBean : subList) {
                        int slen = sectionBean.getsLen();
                        sLen2SubInfo.put(slen, sectionBean);
                    }
                }
                converterRtic2NilinkNoSpeed(degree, linkList, sLen2SubInfo, time, link2Degree,
                        travelTime, sumlen);
                Set<Entry<String, String>> entrySet = link2Degree.entrySet();
                for (Entry<String, String> entry : entrySet) {
                    String key = entry.getKey() + "_" + version + timeSeq;
                    String value = entry.getValue();
                    if (key.length() > 0) {
                        redis.pipeSet(key, value);
                    }
                }
            }
            redis.commit();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            redis.close();
        }
    }

    public static void main(String[] args) {
        File files = new File(args[0]);
        File[] listFiles = files.listFiles();
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            for (File rtic : listFiles) {
                System.out.println(rtic);
                File output = new File(rtic.getParentFile().getAbsolutePath() + "\\output\\");
                if (!output.exists()) {
                    output.mkdir();
                }
                br = new BufferedReader(new FileReader(rtic));
                bw =
                        new BufferedWriter(new FileWriter(rtic.getParentFile().getAbsolutePath()
                                + "\\output\\" + rtic.getName() + "_out"));
                String line;
                Map<String, String> link2Degree = new HashMap<>();
                while ((line = br.readLine()) != null) {
                    String[] split = line.split(",");
                    long time = Long.parseLong(split[2].trim());
                    String meshid = split[3].trim();
                    String rticNum = split[4].trim();
                    String rticKind = split[6].trim();
                    int travelTime = Integer.parseInt(split[7].trim());
                    int degree = Integer.parseInt(split[8].trim());
                    int sectionCount = Integer.parseInt(split[9].trim());
                    String ritcID = getRticID(meshid, rticKind, rticNum);
                    List<LinkInfo> linkList = rtic2Links.get(Long.parseLong(ritcID));
                    int sumlen = 0;
                    if (linkList == null) {
                        continue;
                    }
                    for (LinkInfo linkInfo : linkList) {
                        int len = linkInfo.getLen();
                        sumlen += len;
                    }
                    Map<Integer, SectionBean> sLen2SubInfo = new HashMap<>();
                    if (sectionCount > 0) {
                        Set<SectionBean> subList = getSection(line, sectionCount, sumlen);
                        for (SectionBean sectionBean : subList) {
                            int slen = sectionBean.getsLen();
                            sLen2SubInfo.put(slen, sectionBean);
                        }
                    }
                    converterRtic2NilinkNoSpeed(degree, linkList, sLen2SubInfo, time, link2Degree,
                            travelTime, sumlen);
                    // Set<Entry<String, String>> entrySet = link2Degree.entrySet();
                    // for (Entry<String, String> entry : entrySet) {
                    // String key = entry.getKey();
                    // String value = entry.getValue();
                    // if (key.length() > 0) {
                    // bw.write(key + " : " + value + "\n");
                    // }
                    // }
                }
                String sp1 = link2Degree.get("6429941");
                String sp2 = link2Degree.get("85319921");
                String sp3 = link2Degree.get("6313701");
                String sp4 = link2Degree.get("6429850");
                String sp5 = link2Degree.get("6182409");
                String sp6 = link2Degree.get("6170050");
                String sp7 = link2Degree.get("6570986");
                String sp8 = link2Degree.get("91149144");


                bw.write("6429941" + " : " + sp1 + "\n");
                bw.write("85319921" + " : " + sp2 + "\n");
                bw.write("6313701" + " : " + sp3 + "\n");
                bw.write("6429850" + " : " + sp4 + "\n");
                bw.write("6182409" + " : " + sp5 + "\n");
                bw.write("6170050" + " : " + sp6 + "\n");
                bw.write("6570986" + " : " + sp7 + "\n");
                bw.write("91149144" + " : " + sp8 + "\n");
                // 85319921 6313701 6429850 6182409 6170050

                bw.flush();
            }
            bw.flush();
            br.close();
            bw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
