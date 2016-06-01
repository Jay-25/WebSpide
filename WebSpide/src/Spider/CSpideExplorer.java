/**
 * @Title: CSpider.java
 * @Package Spider
 * @Description: TODO
 * @author
 * @date 2016-5-10 下午3:06:53
 * @version V1.0
 */
package Spider;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import org.apache.commons.logging.LogFactory;
import org.apache.http.auth.AuthScope;
import org.apache.logging.log4j.Logger;

import Log.CLog;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.CollectingAlertHandler;
import com.gargoylesoftware.htmlunit.IncorrectnessListener;
import com.gargoylesoftware.htmlunit.InteractivePage;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.ProxyConfig;
import com.gargoylesoftware.htmlunit.ScriptException;
import com.gargoylesoftware.htmlunit.SilentCssErrorHandler;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebResponse;
import com.gargoylesoftware.htmlunit.html.HTMLParserListener;
import com.gargoylesoftware.htmlunit.javascript.JavaScriptErrorListener;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Spider.CSpider
 * @author：Zhao Jietong
 * @Create：2016-5-10 下午3:06:53
 * @version V1.0
 */
public class CSpideExplorer extends WebClient {

	private static final long                      serialVersionUID         = 5504977916712919939L;
	private static Logger                          logger                   = CLog.getLogger();
	private CSpideExplorerPool.PooledClientFactory ownerPooledClientFactory = null;
	private static boolean                         initStatus               = false;

	public CSpideExplorer() throws Exception {
		this(BrowserVersion.CHROME);
	}

	public CSpideExplorer(BrowserVersion explorer) {
		super(explorer);
		if (!initStatus) {
			initStatus = true;
			java.util.logging.Logger.getLogger("org.apache.http").setLevel(java.util.logging.Level.OFF);
			java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(java.util.logging.Level.OFF);
			java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(java.util.logging.Level.OFF);
			java.util.logging.Logger.getLogger("org.apache.commons.httpclient").setLevel(java.util.logging.Level.OFF);
			LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
		}
		initBrowser();
	}

	@Override
	protected void finalize() throws Throwable {
		this.closeFinal();
		super.finalize();
	}

	protected void initBrowser() {
		setJavaScriptTimeout(30 * 1000);
		waitForBackgroundJavaScript(30 * 1000);
		waitForBackgroundJavaScriptStartingBefore(30 * 1000);
		//
		setJavaScriptErrorListener(new JavaScriptErrorListener() {

			@Override
			public void loadScriptError(InteractivePage arg0, URL arg1, Exception arg2) {
				// TODO Auto-generated method stub
			}

			@Override
			public void malformedScriptURL(InteractivePage arg0, String arg1, MalformedURLException arg2) {
				// TODO Auto-generated method stub
			}

			@Override
			public void scriptException(InteractivePage arg0, ScriptException arg1) {
				// TODO Auto-generated method stub
			}

			@Override
			public void timeoutError(InteractivePage arg0, long arg1, long arg2) {
				// TODO Auto-generated method stub
			}
		});
		setIncorrectnessListener(new IncorrectnessListener() {

			@Override
			public void notify(String arg0, Object arg1) {
				// TODO Auto-generated method stub
			}
		});
		setHTMLParserListener(new HTMLParserListener() {

			@Override
			public void error(String arg0, URL arg1, String arg2, int arg3, int arg4, String arg5) {
				// TODO Auto-generated method stub
			}

			@Override
			public void warning(String arg0, URL arg1, String arg2, int arg3, int arg4, String arg5) {
				// TODO Auto-generated method stub
			}
		});
		setCssErrorHandler(new SilentCssErrorHandler());
		setAjaxController(new NicelyResynchronizingAjaxController());
		setAlertHandler(new CollectingAlertHandler(new ArrayList<String>()));
		//
		getOptions().setThrowExceptionOnScriptError(false);
		getOptions().setPrintContentOnFailingStatusCode(false);
		getOptions().setThrowExceptionOnFailingStatusCode(false);
		//
		getOptions().setTimeout(90 * 1000);
		getOptions().setCssEnabled(false);
		getOptions().setActiveXNative(false);
		getOptions().setAppletEnabled(false);
		getOptions().setUseInsecureSSL(true);
		getOptions().setRedirectEnabled(true);
		getOptions().setDoNotTrackEnabled(true);
		getOptions().setJavaScriptEnabled(false);
		getOptions().setGeolocationEnabled(false);
		getOptions().setPopupBlockerEnabled(false);
		//
		getCookieManager().setCookiesEnabled(true);
		//
		getCurrentWindow().setInnerHeight(Integer.MAX_VALUE);
		getCurrentWindow().setInnerWidth(5000);
		getCurrentWindow().setOuterHeight(Integer.MAX_VALUE);
		getCurrentWindow().setOuterWidth(5000);
	}

	protected void setOwnerPooledClientFactory(CSpideExplorerPool.PooledClientFactory ownerPooledClientFactory) {
		this.ownerPooledClientFactory = ownerPooledClientFactory;
	}

	@Override
	public void close() {
		try {
			if (ownerPooledClientFactory != null) {
				ownerPooledClientFactory.returnSpideExplorer(this);
			}
		}
		catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
	}

	public void closeFinal() {
		this.close();
		super.close();
	}

	public static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		}
		catch (Exception e) {
		}
	}

	private InputStream download(String urlString) {
		URL url = null;
		WebRequest wrq = null;
		AuthScope authScope = null;
		try {
			ProxyConfig config = getOptions().getProxyConfig();
			authScope = new AuthScope(config.getProxyHost(), config.getProxyPort());
			url = new URL(urlString);
			wrq = new WebRequest(url);
			wrq.setProxyHost(config.getProxyHost());
			wrq.setProxyPort(config.getProxyPort());
			wrq.setCredentials(getCredentialsProvider().getCredentials(authScope));
			for (com.gargoylesoftware.htmlunit.util.Cookie c : getCookieManager().getCookies()) {
				wrq.setAdditionalHeader("Cookie", c.toString());
			}
			WebResponse wr = getWebConnection().getResponse(wrq);
			return wr.getContentAsStream();
		}
		catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		finally {
			authScope = null;
			wrq = null;
			url = null;
		}
		return null;
	}

	public byte[] downloadToBytes(String urlString) {
		ByteArrayOutputStream swapStream = null;
		InputStream inputStream = null;
		try {
			inputStream = download(urlString);
			swapStream = new ByteArrayOutputStream();
			int byteCount = 0;
			byte[] bytes = new byte[102400];
			while ((byteCount = inputStream.read(bytes)) != -1) {
				swapStream.write(bytes, 0, byteCount);
			}
			return swapStream.toByteArray();
		}
		catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		finally {
			try {
				swapStream.close();
			}
			catch (IOException e) {
			}
			swapStream = null;
			try {
				inputStream.close();
			}
			catch (IOException e) {
			}
			inputStream = null;
		}
		return null;
	}

	public String downloadToString(String urlString) {
		InputStream inputStream = null;
		StringBuffer sb = null;
		byte[] bytes = null;
		try {
			inputStream = download(urlString);
			sb = new StringBuffer();
			int byteCount = 0;
			bytes = new byte[102400];
			while ((byteCount = inputStream.read(bytes)) != -1) {
				sb.append(new String(bytes, 0, byteCount));
			}
			return sb.toString();
		}
		catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		finally {
			bytes = null;
			sb = null;
			try {
				inputStream.close();
			}
			catch (IOException e) {
			}
			inputStream = null;
		}
		return null;
	}

	public boolean downloadToFile(String urlString, String fileName) {
		InputStream inputStream = null;
		File file = null;
		FileOutputStream fos = null;
		byte[] bytes = null;
		try {
			inputStream = download(urlString);
			file = new File(fileName);
			fos = new FileOutputStream(file);
			int byteCount = 0;
			bytes = new byte[102400];
			while ((byteCount = inputStream.read(bytes)) != -1) {
				fos.write(bytes, 0, byteCount);
			}
			fos.flush();
			return true;
		}
		catch (Exception e) {
			logger.warn(e.getMessage(), e);
		}
		finally {
			try {
				fos.close();
			}
			catch (IOException e) {
			}
			fos = null;
			file = null;
			bytes = null;
			try {
				inputStream.close();
			}
			catch (IOException e) {
			}
			inputStream = null;
		}
		return false;
	}
}
