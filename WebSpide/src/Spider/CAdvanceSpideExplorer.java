/**
 * @Title: CAdvanceSpideExplorer.java
 * @Package Spider
 * @Description: TODO
 * @author
 * @date 2016-5-10 下午5:09:25
 * @version V1.0
 */
package Spider;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.TimeZone;

import org.apache.logging.log4j.Logger;

import Log.CLog;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebResponse;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Spider.CAdvanceSpideExplorer
 * @author：Zhao Jietong
 * @Create：2016-5-10 下午5:09:25
 * @version V1.0
 */
public class CAdvanceSpideExplorer {

	private static Logger           logger        = CLog.getLogger();
	private HashMap<String, String> header        = new HashMap<String, String>();
	private CSpideExplorer          spideExplorer = null;

	public CAdvanceSpideExplorer(BrowserVersion explorer) {
		spideExplorer = CSpideExplorerPool.getInstance(BrowserVersion.CHROME).getSpideExplorer();
		//
		header.put("Cache-Control", "max-age=0");
		header.put("Upgrade-Insecure-Requests", "1");
		header.put("Accept-Language", "zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4");
		header.put("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.108 Safari/537.36");
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	public void close() {
		header.clear();
		header = null;
		spideExplorer.close();
	}

	public CSpideExplorer getExplorer() {
		return spideExplorer;
	}

	public void setHeader(String filedName, String value) {
		header.put(filedName, value);
	}

	public HtmlPage getPage(URL url) {
		return getPage(url, 0, 0);
	}

	public HtmlPage getPage(String url) {
		return getPage(url, 0, 0);
	}

	public HtmlPage getPage(URL url, int retry, long ms) {
		HtmlPage page = null;
		WebRequest request = null;
		WebResponse response = null;
		while (retry >= 0) {
			retry--;
			try {
				request = new WebRequest(url);
				Iterator<Entry<String, String>> iter = header.entrySet().iterator();
				while (iter.hasNext()) {
					Entry<String, String> entry = iter.next();
					request.setAdditionalHeader(entry.getKey(), entry.getValue());
				}
				request.setAdditionalHeader("If-Modified-Since", getGMT());
				//
				page = spideExplorer.getPage(request);
				response = page.getWebResponse();
				if (response.getStatusCode() == 503) {
					page = null;
					request = null;
					response = null;
					logger.info("getPage(" + url.toString() + ") => Retry " + retry);
					CSpideExplorer.sleep(ms);
				}
				else {
					logger.info("getPage(" + url.toString() + ")");
					request = null;
					response = null;
					break;
				}
			}
			catch (Exception e) {
				page = null;
				request = null;
				response = null;
				logger.warn(e.getMessage(), e);
				CSpideExplorer.sleep(ms);
			}
		}
		url = null;
		return page;
	}

	public HtmlPage getPage(String urlstr, int retry, long ms) {
		URL url = null;
		try {
			url = new URL(urlstr);
			return getPage(url, 0, 0);
		}
		catch (MalformedURLException e) {
			logger.warn(e.getMessage(), e);
		}
		return null;
	}

	private String getGMT() {
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		Calendar cd = Calendar.getInstance();
		String gmt = sdf.format(cd.getTime());
		sdf = null;
		cd = null;
		return gmt;
	}
}
