/**
 * @Title: CZip.java
 * @Package AppSync
 * @Description: TODO
 * @author
 * @date 2015-12-15 下午5:04:12
 * @version V1.0
 */
package Convert.Compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @Copyright：2015
 * @Project：WebServiceSync3
 * @Description：
 * @Class：AppSync.CZip
 * @author：Zhao Jietong
 * @Create：2015-12-15 下午5:04:12
 * @version V1.0
 */
public class CZip {
	
	private static int BUFFER = 1024 * 128;
	
	/**
	 * 数据压缩
	 * 
	 * @param is
	 * @param os
	 * @throws Exception
	 */
	public static void compress(InputStream is, OutputStream os) throws Exception {
		GZIPOutputStream gos = new GZIPOutputStream(os);
		int count;
		byte data[] = new byte[BUFFER];
		while ((count = is.read(data, 0, BUFFER)) != -1) {
			gos.write(data, 0, count);
		}
		gos.finish();
		gos.flush();
		gos.close();
	}
	
	public static byte[] compress(byte[] data) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// 压缩
		compress(bais, baos);
		byte[] output = baos.toByteArray();
		baos.flush();
		baos.close();
		bais.close();
		return output;
	}
	
	/**
	 * 数据解压缩
	 * 
	 * @param is
	 * @param os
	 * @throws Exception
	 */
	public static void decompress(InputStream is, OutputStream os) throws Exception {
		GZIPInputStream gis = new GZIPInputStream(is);
		int count;
		byte data[] = new byte[BUFFER];
		while ((count = gis.read(data, 0, BUFFER)) != -1) {
			os.write(data, 0, count);
		}
		gis.close();
	}
	
	public static byte[] decompress(byte[] data) throws Exception {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		// 解压缩
		decompress(bais, baos);
		data = baos.toByteArray();
		baos.flush();
		baos.close();
		bais.close();
		return data;
	}
}
