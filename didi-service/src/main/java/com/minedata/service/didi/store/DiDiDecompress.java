package com.minedata.service.didi.store;

// 解压滴滴轨迹数据
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.minedata.service.didi.store.impl.HdfsStore;



public class DiDiDecompress {

    static Map<String, String> maps;
    private static final String MIDDLE_SUFFIX_GZ = "gzing";
    private static final String MIDDLE_SUFFIX_DECODE = "uncompressing";



    @SuppressWarnings("deprecation")
    public static void gz2hdfs(Configuration conf, String inputPath, String outputPath) {
        HdfsStore hdfsHandle = new HdfsStore(conf);
        FSDataOutputStream outputStream = null;
        try {
            hdfsHandle.uncompressZipFile(inputPath, inputPath + "_" + MIDDLE_SUFFIX_GZ);

            FSDataInputStream inputStream =
                    hdfsHandle.hdfs.open(new Path(inputPath + "_" + MIDDLE_SUFFIX_GZ));
            if (hdfsHandle.isExists(outputPath)) {
                System.out.println("hdfsHandle.isExists " + outputPath + " "
                        + hdfsHandle.isExists(outputPath));
                outputStream = hdfsHandle.hdfs.append(new Path(outputPath));
            } else {
                outputStream = hdfsHandle.hdfs.create(new Path(outputPath));
            }
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
            DiDiDecompress.writeFile(inputStream, bw);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                outputStream.close();
                hdfsHandle.deleteFile(inputPath + "_" + MIDDLE_SUFFIX_GZ);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static File gz2file(String intputFile) throws IOException {
        InputStream uncompressFile = GZIPCompressUtil.uncompressFile(intputFile);
        BufferedWriter bw = null;
        File outputFile_gz = null;
        File outputFile_decode = null;
        FileOutputStream fout = null;
        FileInputStream fin = null;
        try {
            String outputFileStr_gz = intputFile + "." + MIDDLE_SUFFIX_GZ;
            System.out.println(outputFileStr_gz);
            outputFile_gz = new File(outputFileStr_gz);
            fout = new FileOutputStream(outputFile_gz);
            IOUtils.copy(uncompressFile, fout);
            fout.flush();
            fout.close();
            uncompressFile.close();
            outputFile_decode = new File(intputFile + "." + MIDDLE_SUFFIX_DECODE);
            bw = new BufferedWriter(new FileWriter(outputFile_decode));
            fin = new FileInputStream(outputFile_gz);
            DiDiDecompress.writeFile(fin, bw);
            outputFile_gz.delete();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fout.close();
            fin.close();
            bw.close();
            outputFile_gz.delete();
        }
        return outputFile_decode;

    }

    public static File gz2fileVersion(String intputFile, String outputFile) throws IOException {
        InputStream uncompressFile = GZIPCompressUtil.uncompressFile(intputFile);
        BufferedWriter bw = null;
        File outputFile_gz = null;
        File outputFile_decode = null;
        FileOutputStream fout = null;
        FileInputStream fin = null;
        try {
            String outputFileStr_gz = intputFile + "." + MIDDLE_SUFFIX_GZ;
            System.out.println(outputFileStr_gz);
            outputFile_gz = new File(outputFileStr_gz);
            fout = new FileOutputStream(outputFile_gz);
            IOUtils.copy(uncompressFile, fout);
            fout.flush();
            fout.close();
            uncompressFile.close();
            outputFile_decode = new File(outputFile);
            bw = new BufferedWriter(new FileWriter(outputFile_decode, true));
            fin = new FileInputStream(outputFile_gz);
            DiDiDecompress.writeFile(fin, bw);
            outputFile_gz.delete();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fout.close();
            fin.close();
            bw.close();
            outputFile_gz.delete();
        }
        return outputFile_decode;

    }


    public static void writeFile(InputStream reader, BufferedWriter writer) throws Exception {
        FSDataInputStream readerHdfs = null;
        if (reader instanceof FSDataInputStream) {
            readerHdfs = (FSDataInputStream) reader;
        }
        // 存储每个UserId数据重复的次数
        int times;
        // 存储每次截取字节数组
        byte[] bufCache;
        // 存储组装好的每条数据
        StringBuffer sb = new StringBuffer();
        // 存储每一行的唯一UserId
        Map<String, int[]> map = new HashMap<>();
        // 存储上一次的经纬度
        int[] ar = new int[2];
        // 插入点
        StringBuilder sber;
        // 存储sber的参数
        String cache;
        // 存储最后四个字段的字节字符串
        String userData, sysTem, car, status;

        byte[] result;
        // 每个UserId的字节数组总长度
        int length;
        // 存储UserId
        String userid;

        while (true) {

            // 存储每个UserId的字节长度
            byte[] by = new byte[4];
            if (readerHdfs != null && readerHdfs.available() > 0) {
                readerHdfs.readFully(by);
            } else {
                reader.read(by);
            }

            length = DiDiDecompress.byte2int(by);

            if (length == 0) {

                break;
            }
            result = new byte[length];

            if (readerHdfs != null && readerHdfs.available() > 0) {
                readerHdfs.readFully(result);
            } else {
                reader.read(result);
            }


            times = (length - 54) / 18;

            bufCache = new byte[32];

            // Userid
            System.arraycopy(result, 0, bufCache, 0, 32);
            userid = new String(bufCache);

            sb.append(userid).append(",");

            // 经度
            System.arraycopy(result, 32, bufCache, 0, 4);
            cache = String.valueOf(DiDiDecompress.byte2int(bufCache));
            sber = new StringBuilder(cache);
            sber.insert(cache.length() - 6, ".");
            sb.append(sber.toString()).append(",");
            ar[0] = DiDiDecompress.byte2int(bufCache);

            // 纬度
            System.arraycopy(result, 36, bufCache, 0, 4);
            cache = String.valueOf(DiDiDecompress.byte2int(bufCache));
            sber = new StringBuilder(cache);
            sber.insert(cache.length() - 6, ".");
            sb.append(sber.toString()).append(",");
            ar[1] = DiDiDecompress.byte2int(bufCache);
            map.put(userid, ar);

            // 时间戳
            System.arraycopy(result, 40, bufCache, 0, 4);
            sb.append(DiDiDecompress.byte2int(bufCache)).append(",");

            // 速度
            System.arraycopy(result, 44, bufCache, 0, 2);
            cache = String.valueOf(DiDiDecompress.byteToShort(bufCache));
            if (!cache.equals("0")) {
                insertDot(sb, cache);

            } else {
                sb.append("0.0").append(",");
            }

            // 方向
            System.arraycopy(result, 46, bufCache, 0, 2);
            cache = String.valueOf(DiDiDecompress.byteToShort(bufCache));
            if (!cache.equals("0")) {
                insertDot(sb, cache);
            } else {
                sb.append("0.0").append(",");
            }

            // 水平精度因子
            System.arraycopy(result, 48, bufCache, 0, 2);
            sb.append(DiDiDecompress.byteToShort(bufCache)).append(",");

            // 用户数据来源、坐标系统、车辆类型、车辆载客状态
            System.arraycopy(result, 50, bufCache, 0, 2);
            userData = DiDiDecompress.byteToBit(bufCache[0]).substring(0, 4);
            sysTem = DiDiDecompress.byteToBit(bufCache[0]).substring(4, 8);
            car = DiDiDecompress.byteToBit(bufCache[1]).substring(0, 4);
            status = DiDiDecompress.byteToBit(bufCache[1]).substring(4, 8);

            sb.append(maps.get(userData)).append(",").append(maps.get(sysTem)).append(",")
                    .append(maps.get(car)).append(",").append(maps.get(status)).append(",");

            // bizstatus
            System.arraycopy(result, 52, bufCache, 0, 2);
            sb.append(DiDiDecompress.byteToShort(bufCache));

            writer.write(sb.toString());
            writer.newLine();
            sb.setLength(0);

            if (times > 0) {

                for (int i = 0; i < times; i++) {
                    sb.append(userid).append(",");


                    System.arraycopy(result, 54 + 18 * i, bufCache, 0, 2);
                    Short x = DiDiDecompress.byteToShort(bufCache);
                    int x1 = map.get(userid)[0] + x;
                    ar[0] = x1;
                    cache = String.valueOf(x1);
                    sber = new StringBuilder(cache);
                    sber.insert(cache.length() - 6, ".");
                    sb.append(sber.toString()).append(",");

                    System.arraycopy(result, 54 + 18 * i + 2, bufCache, 0, 2);
                    Short y = DiDiDecompress.byteToShort(bufCache);
                    int y1 = map.get(userid)[1] + y;
                    ar[1] = y1;
                    cache = String.valueOf(y1);
                    sber = new StringBuilder(cache);
                    sber.insert(cache.length() - 6, ".");
                    sb.append(sber.toString()).append(",");
                    map.put(userid, ar);

                    System.arraycopy(result, 54 + 18 * i + 4, bufCache, 0, 4);
                    sb.append(DiDiDecompress.byte2int(bufCache)).append(",");

                    System.arraycopy(result, 54 + 18 * i + 8, bufCache, 0, 2);
                    cache = String.valueOf(DiDiDecompress.byteToShort(bufCache));
                    if (!cache.equals("0")) {
                        insertDot(sb, cache);
                    } else {
                        sb.append("0.0").append(",");
                    }

                    System.arraycopy(result, 54 + 18 * i + 10, bufCache, 0, 2);
                    cache = String.valueOf(DiDiDecompress.byteToShort(bufCache));
                    if (!cache.equals("0")) {
                        insertDot(sb, cache);
                    } else {
                        sb.append("0.0").append(",");
                    }

                    System.arraycopy(result, 54 + 18 * i + 12, bufCache, 0, 2);
                    sb.append(DiDiDecompress.byteToShort(bufCache)).append(",");

                    System.arraycopy(result, 54 + 18 * i + 14, bufCache, 0, 2);
                    userData = DiDiDecompress.byteToBit(bufCache[0]).substring(0, 4);
                    sysTem = DiDiDecompress.byteToBit(bufCache[0]).substring(4, 8);

                    car = DiDiDecompress.byteToBit(bufCache[1]).substring(0, 4);
                    status = DiDiDecompress.byteToBit(bufCache[1]).substring(4, 8);

                    sb.append(maps.get(userData)).append(",").append(maps.get(sysTem)).append(",")
                            .append(maps.get(car)).append(",").append(maps.get(status)).append(",");

                    System.arraycopy(result, 54 + 18 * i + 16, bufCache, 0, 2);
                    sb.append(DiDiDecompress.byteToShort(bufCache));

                    writer.write(sb.toString());
                    writer.newLine();
                    sb.setLength(0);
                }
            }
        }
        writer.flush();
        writer.close();
        reader.close();

    }

    private static void insertDot(StringBuffer sb, String cache) {
        StringBuilder sber;
        if (cache.length() > 1) {
            sber = new StringBuilder(cache);
            sber.insert(cache.length() - 1, ".");
            sb.append(sber.toString()).append(",");
        } else {
            sber = new StringBuilder(cache);
            sber.insert(cache.length() - 1, "0.");
            sb.append(sber.toString()).append(",");
        }
    }

    public static String byteToBit(byte b) {
        return "" + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1) + (byte) ((b >> 5) & 0x1)
                + (byte) ((b >> 4) & 0x1) + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b) & 0x1);
    }

    public static int byte2int(byte[] res) {

        int s;
        // 最低位
        int s0 = res[0] & 0xff;
        int s1 = res[1] & 0xff;
        int s2 = res[2] & 0xff;
        int s3 = res[3] & 0xff;
        s3 <<= 24;
        s2 <<= 16;
        s1 <<= 8;
        s = s0 | s1 | s2 | s3;
        return s;
    }

    public static short byteToShort(byte[] b) {

        short s;
        // 最低位
        short s0 = (short) (b[0] & 0xff);
        short s1 = (short) (b[1] & 0xff);
        s1 <<= 8;
        s = (short) (s0 | s1);
        return s;

    }

    // 构造map映射 用户数据来源、坐标系统、车辆类型、车辆载客状态 等字段
    static {
        maps = new HashMap<>();
        maps.put("0000", "LOC_GPS");
        maps.put("0001", "LOC_WIFI");
        maps.put("0010", "LOC_OTHER");

        maps.put("0011", "GCJ_02");
        maps.put("0100", "BD_09");
        maps.put("0101", "WGS_84");

        maps.put("0110", "DD_TAXI");
        maps.put("0111", "DD_GS");
        maps.put("1000", "KD_TAXI");
        maps.put("1001", "DD_SHUNFENG");

        maps.put("1010", "DriverNotWorking");
        maps.put("1011", "DriverWorkingWithoutPassenger");
        maps.put("1100", "DriverWorkingWithPassenger");
        maps.put("1101", "DriverWorking");
    }
}
