package CaiShiChang;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import redis.clients.jedis.Jedis;
import DataSet.CDataSet4YiMuTian;
import Extract.Reduce.CHtmlReduce;
import Extract.Reduce.CHtmlTrim;
import Extract.RegexEngine.CRegex;
import Extract.RegexEngine.CRegexEngine;
import Job.CJobQueue;
import OutputQueue.COutputQueue;
import RegexEngine.CRegexLib;
import Spider.CAdvanceSpideExplorer;
import SpiderBase.CSpideDataStruct;
import SpiderBase.SpideEntryBase;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
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
public class CYiMuTian extends SpideEntryBase {
	
	private CSpideDataStruct dataSet            = null;
	private int              spidedDuplicateNum = 0;
	//
	private CJobQueue        outputQueue        = null;
	private CRegexEngine     regexTable         = new CRegexEngine();
	private CHtmlReduce      htmlReduce         = new CHtmlReduce();
	
	public CYiMuTian() {
		loadRegexRule();
	}
	
	@Override
	protected void finalize() throws Throwable {
		dataSet.close();
		dataSet = null;
		outputQueue = null;
		regexTable.clear();
		regexTable = null;
		htmlReduce = null;
		super.finalize();
	}
	
	@Override
	protected void init() {
		dataSet = CDataSet4YiMuTian.createDataSet(this.getClass().getName());
		dataSet.bindRegexTable(regexTable);
		//
		outputQueue = COutputQueue.getOutputQueue(paras.spideConfig.getConfigFile());
		Jedis jedis = outputQueue.getJedis(COutputQueue.MDB_INDEX_DUPLICATE);
		jedis.del(paras.url);
		jedis.close();
	}
	
	@Override
	protected int setThreadNum(HtmlPage page, ArrayList<String> paras) {
		int num = 1;
		try {
			num = Integer.parseInt(paras.get(0));
			if (num < 1) num = 1;
		}
		catch (Exception e) {
		}
		return num;
	}
	
	@Override
	protected HtmlPage nextPage(HtmlPage page, int pageNum) {
		stop();
		return null;
	}
	
	@Override
	protected HashSet<?> setLinks(HtmlPage page, int pageNum) {
		HashSet<String> urlsList = linksStrings(page, "http://hangqing\\.ymt\\.com/jiage/.+");
		if (spidedDuplicateNum < urlsList.size()) spidedDuplicateNum = urlsList.size();
		return urlsList;
	}
	
	private boolean isValid(HtmlPage Mainpage, Object linkItem, int pageNum) {
		return true;
	}
	
	@Override
	protected void parsePage(HtmlPage Mainpage, Object linkItem, int pageNum) {
		if (!isValid(Mainpage, linkItem, pageNum)) {
			return;
		}
		CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
		HtmlPage newsPage = explorer.getPage((String) linkItem);
		CHtmlTrim.removeHidenElement(newsPage);
		List<HtmlElement> bds = newsPage.getDocumentElement().getElementsByAttribute("li", "class", "bd");
		for (HtmlElement bd : bds) {
			String html = CHtmlTrim.replaceDBC2SBC(htmlReduce.reduce(bd.asXml()));
			html = CHtmlTrim.trim(CHtmlTrim.removeHtmlTag(html));
			//System.out.println(html);
			//
			dataSet.clear();
			dataSet.processRegex(html);
			dataSet.setValue("type", paras.spideParas.get(1));
			//
			if (dataSet.isValidData()) {
				String dataJson = dataSet.toJson().toString();
				outputQueue.addJob(COutputQueue.QUEUE_INDEX_OUTPUT, dataJson);
			}
			else {
				logger.warn("Parse Fail [" + (String) linkItem + "]");
			}
			//
			dataSet.print();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void loadRegexRule() {
		regexTable.set("updateTime", CRegexLib.dateTimeRegex("", ""));
		regexTable.set("name", new CRegex("^(.+)价格", 1));
		regexTable.set("market", new CRegex("^.+价格[ ]+(.+?)[ ]+", 1));
		regexTable.set("avgPrice", new CRegex("([0-9.]+)元", 1));
		regexTable.set("unite", new CRegex("[ ]+[0-9.]+(元/((公?斤)|(两)))[ ]+", 1));
	}
}
