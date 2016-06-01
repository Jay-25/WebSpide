package Spider;

/**
 * @Title: CURL.java
 * @Package SpideBase
 * @Description: TODO
 * @author
 * @date 2015-5-5 下午2:53:49
 * @version V1.0
 */

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @Copyright：2015
 * @Project：Spide
 * @Description：
 * @Class：SpideBase.CURL
 * @author：Zhao Jietong
 * @Create：2015-5-5 下午2:53:49
 * @version V1.0
 */

public class CURLAvailability {

	/**
	 * 功能：检测当前URL是否可连接或是否有效,
	 * 
	 * @param urlStr
	 *            指定URL网络地址
	 * @param retry
	 *            最多连接网络次数, 如果都不成功，视为该地址不可用
	 * @return boolean
	 */
	public static boolean isAvailable(String urlStr, int retry) {
		try {
			return CURLAvailability.isAvailable(new URL(urlStr), retry);
		}
		catch (Exception e) {
		}

		return false;
	}

	public static boolean isAvailable(URL url, int retry) {
		boolean state = false;

		if (url == null) {
			return false;
		}

		try {
			HttpURLConnection con;
			while (retry-- > 0) {
				try {
					con = (HttpURLConnection) url.openConnection();
					if (con.getResponseCode() == 200) {
						state = true;
						break;
					}
				}
				catch (Exception ex) {
					continue;
				}
			}
		}
		catch (Exception e) {
		}

		return state;
	}

}
