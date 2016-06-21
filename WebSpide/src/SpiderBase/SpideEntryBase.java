package SpiderBase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Callable;

import net.sf.json.JSONObject;

import org.apache.logging.log4j.Logger;

import Extract.Json.CJson;
import Job.CJobCounter;
import Job.CJobQueue;
import Job.CJobService4WorkerConfig;
import Job.CJobThread;
import Job.IJobConsole;
import Log.CLog;
import PageParser.CPageParse;
import Spider.CAdvanceSpideExplorer;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * @Copyright：2016
 * @Project：WebSpideEntry
 * @Description：
 * @Class：.MyJob1
 * @author：Zhao Jietong
 * @Create：2016-5-18 上午10:19:07
 * @version V1.0
 */
public abstract class SpideEntryBase extends CPageParse implements IJobConsole {
	
	private final CJobCounter jobCounter = new CJobCounter();
	protected boolean         isStop     = false;
	protected static boolean  isStopAll  = false;
	
	protected class Paras {
		
		public CJobService4WorkerConfig spideConfig = null; // ini配置中的SPIDE项
		public CJobQueue                jobQueue    = null; // 记录该URL完成的页数
		public String                   path        = null; // 该作业的class位置
		public String                   jobname     = null; // 该作业的class名称
		public String                   url         = null; // 该作业处理的URL
		public ArrayList<String>        spideParas  = null; // 该作业在sjob文件中配置的参数
		
		@SuppressWarnings("unchecked")
		public Paras(Object... arg0) {
			spideConfig = (CJobService4WorkerConfig) arg0[0];
			jobQueue = (CJobQueue) arg0[1];
			path = (String) arg0[2];
			jobname = (String) arg0[3];
			url = (String) arg0[4];
			spideParas = (ArrayList<String>) arg0[5];
		}
	}
	
	protected static Logger logger = CLog.getLogger();
	protected Paras         paras  = null;
	private HashSet<?>      links  = null;
	
	public SpideEntryBase() {
	}
	
	protected void init() {
	}
	
	@Override
	public boolean run(final Object... arg0) {
		isStop = false;
		isStopAll = false;
		//
		paras = new Paras(arg0);
		init();
		final String key = paras.jobname + "@" + paras.url;
		//
		CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
		HtmlPage page = explorer
		                .getPage(paras.url, paras.spideConfig.getAttempt(), paras.spideConfig
		                                .getAttemptMS());
		//
		if (page == null) {
			explorer.close();
			explorer = null;
			logger.warn("Can't open " + " [" + key + "]");
			return false;
		}
		//
		int pageNum = 0;
		try {
			String json = paras.jobQueue.jedisGet(CJobQueue.MDB_INDEX_LOG, key);
			if (json != null) {
				CJson argJson = new CJson(json);
				argJson.process();
				String jpageNum = argJson.query("./page").toString();
				String jurl = argJson.query("./url").toString();
				argJson = null;
				try {
					pageNum = Integer.parseInt(jpageNum);
				}
				catch (Exception e) {
				}
				if (paras.url.equals(jurl)) {
					if (pageNum > 0) {
						logger.info("Jump Page " + pageNum + " [" + key + "]");
						for (int p = 1; p <= pageNum && page != null; p++) {
							page = nextPage(page, p);
						}
					}
				}
				else {
					paras.url = jurl;
					logger.info("Jump Page " + pageNum + " [" + key + "] -> [" + paras.url + "]");
					page = explorer.getPage(paras.url, paras.spideConfig.getAttempt(), paras.spideConfig
					                .getAttemptMS());
				}
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		explorer.close();
		explorer = null;
		//
		JSONObject json = new JSONObject();
		while (page != null) {
			logger.info(paras.jobname + "(" + page.getUrl().toString() + ")");
			pageNum++;
			final int finalpageNum = pageNum;
			final HtmlPage finalpage = page;
			int threadNum = setThreadNum(finalpage, paras.spideParas);
			links = setLinks(finalpage, finalpageNum);
			logger.info(page.getUrl().toString() + " sub links : " + links.size());
			if (links != null && links.size() > 0) {
				jobCounter.init(links.size(), threadNum);
				for (final Object linkItem : links) {
					if (linkItem == null) continue;
					while (!jobCounter.jobIsRunable() && !isStop && !isStopAll) {
						sleep(5000);
						System.out.println("SpideEntryBase...");
					}
					if (isStop || isStopAll) break;
					//
					jobCounter.decrement();
					try {
						new CJobThread(new Callable<Object>() {
							
							@Override
							public Object call() throws Exception {
								try {
									parsePage(finalpage, linkItem, finalpageNum);
								}
								catch (Exception e) {
									logger.error(e.getMessage(), e);
								}
								catch (Throwable e) {
									logger.error(e.getMessage(), e);
									System.exit(0);
								}
								jobCounter.increment();
								return null;
							}
						}, "Trd-" + getClass().getName() + "-run.FutureTask", paras.spideConfig.getAttempt(), 3 * 1000, paras.spideConfig
						                .getTimeOut(), new CJobThread.ExceptionCallback() {
							
							@Override
							public void run(Exception e) {
								jobCounter.increment();
							}
						}).start();
					}
					catch (Exception e) {
						logger.error(e.getMessage(), e);
						jobCounter.increment();
					}
					catch (Throwable e) {
						logger.error(e.getMessage(), e);
						System.exit(0);
					}
				}
				links.clear();
				links = null;
				while (jobCounter.getJobNum() < threadNum && !isStop && !isStopAll) {
					sleep(50);
				}
			}
			else {
				jobCounter.resetJobNum();
				try {
					new CJobThread(new Callable<Object>() {
						
						@Override
						public Object call() throws Exception {
							try {
								parsePage(finalpage, null, finalpageNum);
							}
							catch (Exception e) {
								logger.error(e.getMessage(), e);
							}
							catch (Throwable e) {
								logger.error(e.getMessage(), e);
								System.exit(0);
							}
							jobCounter.increment();
							return null;
						}
					}, "Trd-" + getClass().getName() + "-run.default", paras.spideConfig.getAttempt(), 3 * 1000, paras.spideConfig
					                .getTimeOut(), new CJobThread.ExceptionCallback() {
						
						@Override
						public void run(Exception e) {
							jobCounter.increment();
						}
					}).start();
				}
				catch (Exception e) {
					logger.error(e.getMessage(), e);
					jobCounter.increment();
				}
				catch (Throwable e) {
					logger.error(e.getMessage(), e);
					System.exit(0);
				}
				while (!jobCounter.jobIsRunable() && !isStop && !isStopAll) {
					sleep(50);
				}
			}
			//
			json.clear();
			json.put("page", finalpageNum);
			json.put("url", page.getUrl().toString());
			paras.jobQueue.jedisSet(CJobQueue.MDB_INDEX_LOG, key, json.toString());
			//
			if (isStop || isStopAll) break;
			//
			logger.info("[ " + page.getUrl().toString() + " ] -> to next page ...");
			page = nextPage(finalpage, finalpageNum);
			if (page != null) {
				logger.info("the next page is [ " + page.getUrl().toString() + " ] ");
			}
			else {
				logger.info("No next page");
			}
		}
		json = null;
		paras.spideParas = null;
		paras.jobQueue.jedisDel(CJobQueue.MDB_INDEX_LOG, key);
		return true;
	}
	
	protected void stop() {
		isStop = true;
	}
	
	public static void stopAll() {
		isStopAll = true;
	}
	
	protected void sleep(long ms) {
		try {
			Thread.sleep(ms);
		}
		catch (Exception e) {
		}
	}
	
	protected int setThreadNum(HtmlPage page, ArrayList<String> paras) {
		return 1;
	}
	
	protected HashSet<?> setLinks(HtmlPage page, int pageNum) {
		return null;
	}
	
	protected HtmlPage nextPage(HtmlPage page, int pageNum) {
		return null;
	}
	
	protected abstract void parsePage(HtmlPage Mainpage, Object linkItem, int pageNum);
}
