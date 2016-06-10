package SecondHouse;

import java.util.ArrayList;
import java.util.HashSet;

import Algorithm.Math.CEncry;
import DataSet.CDataSet4House;
import DateTime.CDateTime;
import Extract.Reduce.CHtmlReduce;
import Extract.Reduce.CHtmlTrim;
import Extract.RegexEngine.CRegex;
import Extract.RegexEngine.CRegexEngine;
import Geography.CDataCoordinate;
import Geography.CGeography;
import Job.CJobQueue;
import OutputQueue.COutputQueue;
import RegexEngine.CRegexLib;
import Spider.CAdvanceSpideExplorer;
import SpiderBase.CSpideDataStruct;
import SpiderBase.SpideEntryBase;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
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
public class Cqfang extends SpideEntryBase {
	
	private CSpideDataStruct dataSet            = null;
	private int              spidedDuplicateNum = 0;
	//
	private CJobQueue        outputQueue        = null;
	private CRegexEngine     regexTable         = new CRegexEngine();
	private CHtmlReduce      htmlReduce         = new CHtmlReduce();
	
	public Cqfang() {
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
		dataSet = CDataSet4House.createDataSet(this.getClass().getName());
		dataSet.bindRegexTable(regexTable);
		//
		outputQueue = COutputQueue.getOutputQueue(paras.spideConfig.getConfigFile());
		outputQueue.jedisDel(COutputQueue.MDB_INDEX_DUPLICATE, paras.url);
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
		try {
			HtmlAnchor htmlNextPage = page.getAnchorByText("下一页");
			if (htmlNextPage != null) {
				return htmlNextPage.click();
			}
		}
		catch (Exception e) {
		}
		return null;
	}
	
	@Override
	protected HashSet<?> setLinks(HtmlPage page, int pageNum) {
		HashSet<String> urlsList = linksStrings(page, "http://chengdu\\.qfang\\.com/sale/\\d+");
		if (spidedDuplicateNum < urlsList.size()) spidedDuplicateNum = urlsList.size();
		return urlsList;
	}
	
	private boolean isValid(HtmlPage Mainpage, Object linkItem, int pageNum) {
		boolean valid = true;
		//
		if (linkItem == null) {
			return false;
		}
		//
		if (outputQueue.jedisExists(COutputQueue.MDB_INDEX_DUPLICATE, paras.url)) {
			if (Integer.parseInt(outputQueue.jedisGet(COutputQueue.MDB_INDEX_DUPLICATE, paras.url)) > spidedDuplicateNum) {
				stop();
				valid = false;
			}
		}
		return valid;
	}
	
	@Override
	protected void parsePage(HtmlPage Mainpage, Object linkItem, int pageNum) {
		if (!isValid(Mainpage, linkItem, pageNum)) {
			return;
		}
		//
		String curUrl = (String) linkItem;
		//
		CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
		HtmlPage curPage = explorer
		                .getPage(curUrl, paras.spideConfig.getAttempt(), paras.spideConfig
		                                .getAttemptMS());
		explorer.close();
		explorer = null;
		//
		CHtmlTrim.removeHidenElement(curPage);
		String html = htmlReduce.reduce(curPage.asXml());
		html = CHtmlTrim.replaceDBC2SBC(html);
		html = CHtmlTrim.removeHtmlTag(html);
		html = CHtmlTrim.trim(html);
		html = html.replaceAll(".*收藏房源", " ");
		html = html.replaceAll("房源点评.*", " ");
		html = html.replaceAll("查看更多.*", " ");
		html = html.replaceAll("小区简介", " ");
		html = html.replaceAll("小区相册", " ");
		html = html.replaceAll("\\[|\\]", "");
		//
		dataSet.clear();
		dataSet.processRegex(html);
		dataSet.setValue("url", curUrl);
		dataSet.setValue("url_cofrom", paras.url);
		dataSet.setValue("raw", html);
		//
		autoSet(dataSet, curUrl);
		//
		if (dataSet.isValidData()) {
			String dataJson = dataSet.toJson().toString();
			outputQueue.addData(COutputQueue.QUEUE_INDEX_OUTPUT, dataJson);
		}
		else {
			logger.warn("Parse Fail [" + curUrl + "]");
		}
		//
		dataSet.print();
	}
	
	@SuppressWarnings("unchecked")
	private void loadRegexRule() {
		// 站内标识
		regexTable.set("web_in_uid", new CRegex("信息编号[:\\s]*(\\w+)", 1), new CRegex("房源编号[:\\s]*(\\w+)", 1), new CRegex("房号编号[:\\s]*(\\w+)", 1));
		// 发布时间
		regexTable.set("release_time", CRegexLib.dateTimeRegex("", ""), CRegexLib
		                .dateTimeOffsetRegex("", ""));
		// 总价
		regexTable.set("price", new CRegex("售价:?\\s*([0-9.]+)\\s*万", 1), new CRegex("\\s*([0-9.]+)\\s*万\\s*单价", 1), new CRegex("总\\s*价[:\\s]*([0-9.]+)\\s*万", 1), new CRegex("房源价格:\\s*([0-9.]+)\\s*万", 1));
		// 单价
		regexTable.set("price_unit", new CRegex("单价[:\\s]*([0-9.]+)\\s*元", 1), new CRegex("\\(([0-9.]+)\\s*元/㎡\\)", 1));
		// 产权
		regexTable.set("house_property", new CRegex("(\\d+年)产权", 1));
		// 户型
		regexTable.set("house_style", CRegexLib.houseStyleRegex("", ""), new CRegex("户型[:\\s]*(\\w*)\\s", 1));
		// 结构
		regexTable.set("house_struct", new CRegex("结\\s*构[:\\s]*([\u0000-\uffff]*?)\\s", 1));
		// 装修
		regexTable.set("house_decoration", CRegexLib.houseDecorationRegex("", ""), new CRegex("装\\s*修[:\\s]*([\u0000-\uffff]*?)\\s", 1));
		// 住宅类别
		regexTable.set("house_class", CRegexLib.houseClassRegex("", ""));
		// 建筑类型
		regexTable.set("build_class", new CRegex("建筑类别[:\\s]*([\u0000-\uffff]*?)\\s", 1));
		// 楼盘名称
		regexTable.set("build_name", new CRegex("楼盘名称[:\\s]*([^\\(\\[]*)[\\s\\(\\[]", 1));
		// 使用面积
		regexTable.set("usage_area", new CRegex("([0-9.]+)㎡", 1));
		// 建筑面积
		regexTable.set("build_area", new CRegex("([0-9.]+)㎡", 1));
		// 年代
		regexTable.set("build_time_year", new CRegex("([0-9]*)年建", 1), new CRegex("建?筑?造?年\\s*代[:\\s]*([0-9]*)", 1));
		// 朝向
		regexTable.set("build_face", CRegexLib.houseFaceRegex("房?间?朝\\s*向[:\\s]*", ""), CRegexLib
		                .houseFaceRegex("朝\\s*", ""));
		// 楼层
		regexTable.set("build_layer", new CRegex("楼层[:\\s]*(\\d+)/\\d+", 1), new CRegex("楼层[:\\s]*(\\w+)层", 1), new CRegex("第(\\d+)层", 1));
		// 总楼层
		regexTable.set("build_max_layer", new CRegex("楼层[:\\s]*\\d+/(\\d+)", 1), new CRegex("楼层[:\\s]*.*/(\\w+)层", 1), new CRegex("共(\\w+)层", 1));
		// 地址-小区
		regexTable.set("address_city", new CRegex("小区名?称?[:\\s]*([\u0000-\uffff]*?)\\s", 1), new CRegex("\\s*([\u0000-\uffff]+?)小区", 1));
		// 地址
		regexTable.set("address", CRegexLib.addressRegex("地\\s*址[:\\s]*", ""), CRegexLib
		                .addressRegex("位于[:\\s]*", ""), CRegexLib.addressRegex("", ""));
		// 开发商
		regexTable.set("develop_company", new CRegex("开\\s*发\\s*商[:\\s]*(\\w+)\\s", 1));
		// 物业费
		regexTable.set("property_costs", new CRegex("物\\s*业\\s*费[:\\s]*([0-9.]*)元", 1));
		// 物业公司
		regexTable.set("property_company", new CRegex("物业公司[:\\s]*([\u0000-\uffff]*?公司)\\s+物业类型", 1));
		// 按揭首付
		regexTable.set("mortgage_down_payment", new CRegex("参?考?首付[:\\s]*([0-9.]*)", 1));
		// 月供
		regexTable.set("monthly", new CRegex("参?考?月供[:\\s]*([0-9.]*)", 1));
	}
	
	private double _round(double d) {
		return Math.round(d * 10000) / 10000;
	}
	
	private void autoSet(CSpideDataStruct dataSet, String... args) {
		if (dataSet.isNull(dataSet.getData("release_time"))) dataSet
		                .setValue("release_time", CDateTime.getCurrentTime("yyyy-MM-dd"));
		dataSet.setValue("address", (paras.spideParas.get(1) + "," + dataSet
		                .getData("build_name", "") + "," + dataSet.getData("address_city", "") + "," + dataSet
		                .getData("address", ""))
		                .replaceAll("(\\pP)\\pP*", "$1"));
		//
		dataSet.setValue("style", "BaiDu");
		CDataCoordinate coordinate = CGeography.getInstance()
		                .getCoordinate_Baidu((String) dataSet.getData("address"));
		if (coordinate != null) {
			dataSet.setValue("longitude", coordinate.getLongitude());
			dataSet.setValue("latitude", coordinate.getLatitude());
			dataSet.setValue("elevation", coordinate.getElevation());
			coordinate = null;
		}
		//
		if (dataSet.isNull(dataSet.getData("web_in_uid"))) {
			dataSet.setValue("web_in_uid", CEncry.md5(args[0]));
		}
		//
		double price = 0.0;
		try {
			price = (double) dataSet.getData("price");
		}
		catch (Exception e) {
		}
		double price_unit = 0.0;
		try {
			price_unit = (double) dataSet.getData("price_unit");
		}
		catch (Exception e) {
		}
		double build_area = 0.0;
		try {
			build_area = (double) dataSet.getData("build_area");
		}
		catch (Exception e) {
		}
		double usage_area = 0.0;
		try {
			usage_area = (double) dataSet.getData("usage_area");
		}
		catch (Exception e) {
		}
		if (price_unit <= 0.0) {
			if (build_area > 0.0 && price > 0.0) {
				price_unit = _round(10000 * price / build_area);
			}
			else if (usage_area > 0.0 && price > 0.0) {
				price_unit = _round(10000 * price / usage_area);
			}
		}
		if (build_area <= 0.0 && price_unit > 0.0 && price > 0.0) {
			build_area = _round(price * 10000 / price_unit);
		}
		if (price <= 0.0) {
			if (price_unit > 0.0 && build_area > 0.0) {
				price = _round(price_unit * build_area / 10000);
			}
			else if (price_unit > 0.0 && usage_area > 0.0) {
				price = _round(price_unit * usage_area / 10000);
			}
		}
		dataSet.setValue("price", price);
		dataSet.setValue("price_unit", price_unit);
		dataSet.setValue("build_area", build_area);
		dataSet.setValue("usage_area", usage_area);
	}
}
