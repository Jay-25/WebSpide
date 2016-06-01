/**
 * @Title: CDownloader.java
 * @Package SpideBase
 * @Description: TODO
 * @author
 * @date 2015-5-5 下午2:12:25
 * @version V1.0
 */
package Spider;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import System.Directory.CFile;

/**
 * @Copyright：2015
 * @Project：Spide
 * @Description：
 * @Class：SpideBase.CDownloader
 * @author：Zhao Jietong
 * @Create：2015-5-5 下午2:12:25
 * @version V1.0
 */
public class CDownloader {
	
	public static interface CAfterDownload {
		
		public void doAfter(final String downloadFileName);
		
		public void doFail(final String url, final String downloadFileName);
	}
	
	private ExecutorService fixedThreadPool = null;
	private int             threadPoolSize  = 3;
	private String          downPath        = "";
	
	public CDownloader() {
	}
	
	public void setThreadPoolSize(int threadPoolSize) {
		this.threadPoolSize = threadPoolSize;
	}
	
	public String getDownloadPath() {
		return downPath;
	}
	
	public void setDownloadPath(String downPath) {
		this.downPath = downPath;
		File dirJob = new File(downPath);
		if (!dirJob.isDirectory()) {
			dirJob.mkdirs();
		}
		dirJob = null;
	}
	
	private String renameFileName(String fileName) {
		return downPath + File.separator + CFile.fixFileName(fileName, "_");
	}
	
	public String downloadImmediately(String url, String downloadFileName) {
		if (!CURLAvailability.isAvailable(url, 3)) {
			return null;
		}
		String fileName = renameFileName(downloadFileName);
		try {
			CSpideExplorer explorer = new CSpideExplorer();
			explorer.downloadToFile(url, fileName);
			explorer.close();
			explorer = null;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return fileName;
	}
	
	public void addTask(final String url, String downloadFileName, final CAfterDownload afterDownload) {
		if (fixedThreadPool == null) {
			fixedThreadPool = Executors.newFixedThreadPool(threadPoolSize);
		}
		final String fileName = renameFileName(downloadFileName);
		if (!CURLAvailability.isAvailable(url, 3)) {
			if (afterDownload != null) {
				afterDownload.doFail(url, fileName);
			}
			return;
		}
		fixedThreadPool.execute(new Runnable() {
			
			@Override
			public void run() {
				try {
					CSpideExplorer explorer = new CSpideExplorer();
					explorer.downloadToFile(url, fileName);
					explorer.close();
					explorer = null;
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				if (afterDownload != null) {
					afterDownload.doAfter(fileName);
				}
			}
		});
	}
	
	public void shutdown() {
		if (fixedThreadPool == null) {
			return;
		}
		fixedThreadPool.shutdown();
		try {
			fixedThreadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		fixedThreadPool = null;
	}
	// public static void main(String[] args) {
	// String url =
	// "http://www.hibor.com.cn/webdownload.asp?doctitle=%B9%E3%B7%A2%D6%A4%C8%AF%2DA%B9%C92014%C4%EA%B1%A8%BA%CD2015%D2%BB%BC%BE%B1%A8%C9%EE%B6%C8%B7%D6%CE%F6%A3%BA%B4%F3%CA%B1%B4%FA%D5%BD%CA%A4%D0%A1%D6%DC%C6%DA%2D150502&uname=&did=1576697&degree=1&baogaotype=4&fromurl=http%3A%2F%2Fwww%2Ehibor%2Ecom%2Ecn%2Fdocdetail%2Easp%3Fid%3D1576697&fromtype=21";
	//
	// CDownloader downloader = new CDownloader();
	// downloader.setDownloadPath("D:\\R\\DragonSpide\\Spide\\Spide\\download");
	// downloader.addTask(url, "a1.pdf", new CAfterDownload() {
	//
	// @Override
	// public void doAfter(String fileName) {
	// CPdf2Text pdf2Text = new
	// CPdf2Text("D:\\R\\DragonSpide\\Spide\\Spide\\xpdf\\xpdfbin-win-3.04\\bin64");
	// pdf2Text.setCmdParas("-raw -nopgbrk -clip -table -enc GBK -q");
	// pdf2Text.setFile(fileName);
	// String result = pdf2Text.convertText();
	// System.out.println(result);
	// }
	//
	// @Override
	// public void doFail(String url, String downloadFileName) {
	// }
	//
	// });
	// downloader.shutdown();
	// }
}
