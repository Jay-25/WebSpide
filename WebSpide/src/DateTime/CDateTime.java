/**
 * @Title: CDateTime.java
 * @Package DateTime
 * @Description: TODO
 * @author
 * @date 2016-5-19 下午1:20:28
 * @version V1.0
 */
package DateTime;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：DateTime.CDateTime
 * @author：Zhao Jietong
 * @Create：2016-5-19 下午1:20:28
 * @version V1.0
 */
public class CDateTime {
	
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static String getCurrentTime(String format) {
		Calendar rightNow = Calendar.getInstance();
		Date date = new Date();
		rightNow.setTime(date);
		date = null;
		DateFormat _df = new SimpleDateFormat(format);
		String rString = _df.format(rightNow.getTime());
		_df = null;
		return rString;
	}
	
	public static double compareTime(String t1, String t2) {
		try {
			return (df.parse(t1).getTime() - df.parse(t2).getTime()) / 1000.0;
		}
		catch (Exception e) {
		}
		return -1;
	}
}
