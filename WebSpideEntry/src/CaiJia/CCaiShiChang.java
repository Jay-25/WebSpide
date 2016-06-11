package CaiJia;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import DataSet.CDataSet4CaiShiChang;
import Extract.Json.CJson;
import Job.CJobCounter;
import Job.CJobQueue;
import Job.CJobThread;
import OutputQueue.COutputQueue;
import SpiderBase.CSpideDataStruct;
import SpiderBase.SpideEntryBase;

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
public class CCaiShiChang extends SpideEntryBase {
	
	private CSpideDataStruct dataSet     = null;
	//
	private CJobQueue        outputQueue = null;
	
	//
	private class CGetNutrition extends CJson {
		
		public CGetNutrition(String url, String encode) throws UnsupportedEncodingException,
		                IOException {
			super(new URL(url), encode);
		}
		
		@Override
		public void doObject(final JObject jObject) {
			// seeJObject(jObject);
			String food = (String) query(jObject, "./data/name");
			JObject nus = (JObject) query(jObject, "./data/nutritionDesciption");
			if (nus != null) {
				String per = (String) query(nus, "per");
				JArray obj_nu = (JArray) query(nus, "content");
				for (int i = 0; i < obj_nu.size(); i++) {
					JObject nu = (JObject) obj_nu.get(i);
					String unit = (String) query(nu, "unit");
					String name = (String) query(nu, "key");
					String value = (String) query(nu, "value");
					int nrv = (Integer) query(nu, "nrv");
					//
					dataSet.clear();
					dataSet.setValue("job_name", this.getClass().getName());
					dataSet.setValue("dataStyle", 1);
					dataSet.setValue("food", food.trim());
					dataSet.setValue("name", name.trim());
					dataSet.setValue("unit", unit.trim());
					dataSet.setValue("val", value.trim());
					dataSet.setValue("nrv", nrv);
					dataSet.setValue("per", per.trim());
					//
					String dataJson = dataSet.toJson().toString();
					outputQueue.addData(COutputQueue.QUEUE_INDEX_OUTPUT, dataJson);
					dataSet.print();
				}
			}
		}
	}
	
	//
	private class CGetPrice extends CJson {
		
		private final CJobCounter jobCounter = new CJobCounter();
		
		public CGetPrice(String url, String encode) throws UnsupportedEncodingException,
		                IOException {
			super(new URL(url), encode);
			jobCounter.init(setThreadNum(null, paras.spideParas));
		}
		
		@Override
		public void doObject(final JObject jObject) {
			// seeJObject(jObject);
			String updateTime = (String) query(jObject, "./data/updateTime");
			String market = (String) query(jObject, "./data/market");
			JArray obj_ps = (JArray) query(jObject, "./data/price");
			for (int i = 0; i < obj_ps.size(); i++) {
				String type = (String) query((JObject) obj_ps.get(i), "name");
				JArray obj_fps = (JArray) query((JObject) obj_ps.get(i), "foods");
				for (int f = 0; f < obj_fps.size(); f++) {
					JObject obj_food = (JObject) obj_fps.get(f);
					final String name = (String) query(obj_food, "name");
					double avgPrice = (Double) query(obj_food, "avgPrice");
					double maxPrice = (Double) query(obj_food, "maxPrice");
					double minPrice = (Double) query(obj_food, "minPrice");
					Boolean average = (Boolean) query(obj_food, "average");
					//
					dataSet.clear();
					dataSet.setValue("job_name", this.getClass().getName());
					dataSet.setValue("dataStyle", 0);
					dataSet.setValue("updateTime", updateTime.trim());
					dataSet.setValue("market", market.trim());
					dataSet.setValue("type", type.trim());
					dataSet.setValue("name", name.trim());
					dataSet.setValue("avgPrice", avgPrice);
					dataSet.setValue("maxPrice", maxPrice);
					dataSet.setValue("minPrice", minPrice);
					dataSet.setValue("average", average ? "1" : "0");
					//
					String dataJson = dataSet.toJson().toString();
					outputQueue.addData(COutputQueue.QUEUE_INDEX_OUTPUT, dataJson);
					dataSet.print();
					//
					jobCounter.decrement();
					new CJobThread(new Callable<Object>() {
						
						@Override
						public Object call() throws Exception {
							CGetNutrition getNutrition = null;
							try {
								getNutrition = new CGetNutrition("http://app.95e.com/vm/getMaterial2.aspx?name=" + URLEncoder
								                .encode(name, "UTF-8"), "UTF-8");
								getNutrition.process();
							}
							catch (Exception e) {
								e.printStackTrace();
							}
							getNutrition = null;
							jobCounter.increment();
							return null;
						}
					}, "Trd-" + getClass().getName() + "-CGetPrice", paras.spideConfig.getTimeOut())
					                .start();
					//
					while (!jobCounter.jobIsRunable() && !isStop) {
						sleep(50);
					}
					if (isStop) break;
				}
			}
		}
	}
	
	//
	private class CGetDistrict extends CJson {
		
		private final CJobCounter jobCounter = new CJobCounter();
		
		public CGetDistrict(String url, String encode) throws UnsupportedEncodingException,
		                IOException {
			super(new URL(url), encode);
			jobCounter.init(setThreadNum(null, paras.spideParas));
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public void doObject(final JObject jObject) {
			// seeJObject(jObject);
			JArray obj_d = (JArray) query(jObject, "./data");
			for (int i = 0; i < obj_d.size(); i++) {
				String district = (String) query((JObject) obj_d.get(i), "district");
				JObject obj_mrkType = (JObject) query((JObject) obj_d.get(i), "markets");
				Iterator iter = obj_mrkType.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry entry = (Map.Entry) iter.next();
					String type = (String) entry.getKey();
					JArray obj_mks = (JArray) entry.getValue();
					for (int k = 0; k < obj_mks.size(); k++) {
						Boolean updated = (Boolean) query((JObject) obj_mks.get(k), "updated");
						final String name = (String) query((JObject) obj_mks.get(k), "name");
						//
						dataSet.clear();
						dataSet.setValue("job_name", this.getClass().getName());
						dataSet.setValue("dataStyle", 2);
						dataSet.setValue("district", district.trim());
						dataSet.setValue("type", type.trim());
						dataSet.setValue("name", name.trim());
						dataSet.setValue("updated", updated ? "1" : "0");
						//
						String dataJson = dataSet.toJson().toString();
						outputQueue.addData(COutputQueue.QUEUE_INDEX_OUTPUT, dataJson);
						dataSet.print();
						//
						jobCounter.decrement();
						new CJobThread(new Callable<Object>() {
							
							@Override
							public Object call() throws Exception {
								CGetPrice getPrice = null;
								try {
									getPrice = new CGetPrice("http://app.95e.com/vm/getPrice2.aspx?m=" + URLEncoder
									                .encode(name, "UTF-8"), "UTF-8");
									getPrice.process();
								}
								catch (Exception e) {
									logger.error(e.getMessage(), e);
								}
								getPrice = null;
								jobCounter.increment();
								return null;
							}
						}, "Trd-" + getClass().getName() + "-CGetDistrict", paras.spideConfig.getTimeOut())
						                .start();
						//
						while (!jobCounter.jobIsRunable() && !isStop) {
							sleep(50);
						}
						if (isStop) break;
					}
				}
			}
		}
	}
	
	public CCaiShiChang() {
	}
	
	@Override
	protected void finalize() throws Throwable {
		dataSet.close();
		dataSet = null;
		outputQueue = null;
		super.finalize();
	}
	
	@Override
	protected void init() {
		dataSet = CDataSet4CaiShiChang.createDataSet(this.getClass().getName());
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
	protected void parsePage(HtmlPage Mainpage, Object linkItem, int pageNum) {
	}
	
	@Override
	public boolean run(final Object... arg0) {
		isStop = false;
		//
		paras = new Paras(arg0);
		init();
		//
		CGetDistrict getDistrict;
		try {
			getDistrict = new CGetDistrict("http://app.95e.com/vm/getMarkets2.aspx", "UTF-8");
			getDistrict.process();
			getDistrict = null;
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		//
		return true;
	}
}
