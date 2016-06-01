/**
 * @Title: CFile.java
 * @Package System.Directory
 * @Description: TODO
 * @author
 * @date 2015-5-6 下午2:38:54
 * @version V1.0
 */
package System.Directory;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

/**     
 * @Copyright：2015
 * @Project：Spide     
 *  
 * @Description：  
 * @Class：System.Directory.CFile       
 * @author：Zhao Jietong
 * @Create：2015-5-6 下午2:38:54     
 * @version   V1.0      
 */
public class CFile {

	public static String fixFileName(String fileName, String fix) {
		return fileName.replaceAll("[\\\\^\\\\/\\:*?\"<>|&]", fix);
	}

	public static String getFileExtension(File file) {
		String fileName = file.getName();
		if (fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
			return fileName.substring(fileName.lastIndexOf(".") + 1);
		}
		else {
			return "";
		}
	}

	public static String getFileExtension(String fileName) {
		return getFileExtension(new File(fileName));
	}

	/**
	 * 读取修改时间的方法
	 */
	public static Date getModifiedTime(File f) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(f.lastModified());
		return cal.getTime();
	}
}
