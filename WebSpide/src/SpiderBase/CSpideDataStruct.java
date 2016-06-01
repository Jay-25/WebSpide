package SpiderBase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;

import org.apache.logging.log4j.Logger;

import Extract.RegexEngine.CRegexEngine;
import Log.CLog;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：SpideBase.DataStruct.CDataStructUsingRegexEngine
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:07:37
 * @version V1.0
 */
public class CSpideDataStruct {
	
	private static Logger                  logger         = CLog.getLogger();
	private final HashMap<String, Object>  dataDefaultSet = new HashMap<String, Object>();
	private final HashMap<String, String>  dataTypeSet    = new HashMap<String, String>();
	private final HashMap<String, Boolean> dataIsNullSet  = new HashMap<String, Boolean>();
	private final HashMap<String, Object>  dataSet        = new HashMap<String, Object>();
	private CRegexEngine                   regexTable     = null;
	
	public CSpideDataStruct() {
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
	
	public void close() {
		dataDefaultSet.clear();
		dataTypeSet.clear();
		dataIsNullSet.clear();
		dataSet.clear();
	}
	
	public void bindRegexTable(CRegexEngine regexTable) {
		this.regexTable = regexTable;
	}
	
	public JSONObject toJson() {
		return JSONObject.fromObject(dataSet);
	}
	
	public boolean isNull(Object obj) {
		if (obj == null || obj.toString().length() <= 0) return true;
		return false;
	}
	
	public HashMap<String, Object> getDataSet() {
		return dataSet;
	}
	
	public Object getData(String key) {
		return dataSet.get(key);
	}
	
	public Object getData(String key, Object defaultValue) {
		Object result = dataSet.get(key);
		return isNull(result) ? defaultValue : result;
	}
	
	public HashMap<String, String> getDataTypeSet() {
		return dataTypeSet;
	}
	
	public String getDataType(String key) {
		return dataTypeSet.get(key);
	}
	
	public void setValue(String key, Object value) {
		dataSet.put(key, value);
	}
	
	public void defineColumn(String key, String type, Boolean isNull, Object defaultValue) {
		dataSet.put(key, null);
		dataIsNullSet.put(key, isNull);
		dataTypeSet.put(key, type);
		dataDefaultSet.put(key, defaultValue);
	}
	
	public void processRegex(final String text) {
		processRegex(text, null);
	}
	
	public void processRegex(final String text, final String field) {
		ExecutorService pool = Executors.newCachedThreadPool();
		Iterator<Entry<String, Object>> iter = dataSet.entrySet().iterator();
		while (iter.hasNext()) {
			final Entry<String, Object> entry = iter.next();
			final String dataName = entry.getKey();
			if (field != null && !field.equals(dataName)) continue;
			if (regexTable.get(dataName) == null) continue;
			pool.execute(new Runnable() {
				
				@Override
				public void run() {
					try {
						Object dataValue = entry.getValue();
						String dataType = dataTypeSet.get(dataName);
						String result = regexTable.toRegex(dataName, text);
						if (dataType.equals("String")) {
							dataValue = result;
						}
						else if (dataType.equals("Integer") || dataType.equals("int")) {
							dataValue = Integer.parseInt(result);
						}
						else if (dataType.equals("Double") || dataType.equals("double")) {
							dataValue = Double.parseDouble(result);
						}
						else if (dataType.equals("Float") || dataType.equals("float")) {
							dataValue = Float.parseFloat(result);
						}
						else if (dataType.equals("Character") || dataType.equals("char")) {
							dataValue = Integer.parseInt(result);
						}
						else {
							dataValue = result;
						}
						dataSet.put(dataName, dataValue);
					}
					catch (Exception e) {
						dataSet.put(dataName, null);
					}
				}
			});
		}
		pool.shutdown();
		try {
			pool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		pool = null;
	}
	
	public void clear() {
		Iterator<String> iter = dataSet.keySet().iterator();
		while (iter.hasNext()) {
			String dataName = iter.next();
			dataSet.put(dataName, dataDefaultSet.get(dataName));
		}
	}
	
	public boolean isValidData() {
		Iterator<Entry<String, Object>> iter = dataSet.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String dataName = entry.getKey();
			Object dataValue = entry.getValue();
			String dataType = dataTypeSet.get(dataName);
			boolean dataIsNull = dataIsNullSet.get(dataName);
			if (!dataIsNull && (dataValue == null || dataValue.toString().length() <= 0)) return false;
			if (dataValue != null) {
				try {
					if (dataType.equals("Integer") || dataType.equals("int")) {
						Integer.parseInt(dataValue.toString());
					}
					else if (dataType.equals("Double") || dataType.equals("double")) {
						Double.parseDouble(dataValue.toString());
					}
					else if (dataType.equals("Float") || dataType.equals("float")) {
						Float.parseFloat(dataValue.toString());
					}
					else if (dataType.equals("Character") || dataType.equals("char")) {
						Integer.parseInt(dataValue.toString());
					}
				}
				catch (Exception e) {
					return false;
				}
			}
		}
		return true;
	}
	
	public void print() {
		StringBuffer str = new StringBuffer();
		Iterator<Entry<String, Object>> iter = dataSet.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, Object> entry = iter.next();
			String dataName = entry.getKey();
			Object dataValue = entry.getValue();
			str.append(dataName + " : " + dataValue + "\n");
		}
		str.append("\n-----------\n");
		System.out.println(str.toString());
		str = null;
	}
}
