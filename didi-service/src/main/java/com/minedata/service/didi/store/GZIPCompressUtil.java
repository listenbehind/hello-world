package com.minedata.service.didi.store;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

public class GZIPCompressUtil {

	private final static Logger log = Logger.getLogger(GZIPCompressUtil.class);

	/**
	 * 
	 * 压缩文件
	 * 
	 * @param inFileName 文件名加上绝对路径
	 */
	public static void compressFile(String inFileName) {

		try {
			log.info("创建GZIP压缩流");
			String outFileName = inFileName + ".gz";
			GZIPOutputStream out = null;
			
			out = new GZIPOutputStream(new FileOutputStream(outFileName));

			log.info("打开输入文件");
			FileInputStream in = new FileInputStream(inFileName);

			byte[] buf = new byte[1024];
			int len = 0;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();

			log.info(inFileName + "文件压缩完成");
			out.finish();
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * 
	 * 解压文件
	 * 
	 * @param inFileName 文件名加绝对路径
	 * @return 解压文件后的文件输入流
	 */

	public static InputStream uncompressFile(String inFileName) {

		GZIPInputStream in = null;

		try {
			if (!getExtension(inFileName).equalsIgnoreCase("gz")) {
				log.error("请输入正确的压缩文件，以gz结尾");
			}

			in = new GZIPInputStream(new FileInputStream(inFileName));

		} catch (IOException e) {
			e.printStackTrace();
			try {
				in.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}

		return in;

	}

	public static String getExtension(String f) {
		String ext = "";
		int i = f.lastIndexOf('.');

		if (i > 0 && i < f.length() - 1) {
			ext = f.substring(i + 1);
		}
		return ext;
	}

}
