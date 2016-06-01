package Extract.Json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;

public class CJson {
	
	public class JObject extends HashMap<String, Object> {
		
		private static final long serialVersionUID = 8842027466912574334L;
	}
	
	public class JArray extends ArrayList<Object> {
		
		private static final long serialVersionUID = 5945711917334569751L;
	}
	
	private String        json    = "";
	private JSONObject    jobj    = null;
	private final JObject jObject = new JObject();
	
	public CJson() {
	}
	
	public CJson(String json) {
		this.json = json;
	}
	
	public CJson(File file, String encode) throws IOException {
		String line;
		FileInputStream fi = new FileInputStream(file);
		InputStreamReader ireader = new InputStreamReader(fi, encode);
		BufferedReader in = new BufferedReader(ireader);
		StringBuffer pageBuffer = new StringBuffer();
		while ((line = in.readLine()) != null) {
			pageBuffer.append(line);
		}
		in.close();
		this.json = pageBuffer.toString();
		//
		pageBuffer = null;
		in = null;
		ireader.close();
		ireader = null;
		fi.close();
		fi = null;
	}
	
	public CJson(URL url, String encode) throws UnsupportedEncodingException, IOException {
		InputStreamReader ireader = null;
		BufferedReader in = null;
		StringBuffer pageBuffer = null;
		try {
			String line;
			ireader = new InputStreamReader(url.openStream(), encode);
			in = new BufferedReader(ireader);
			pageBuffer = new StringBuffer();
			while ((line = in.readLine()) != null) {
				pageBuffer.append(line);
			}
			in.close();
			this.json = pageBuffer.toString();
			//
		}
		catch (ConnectException e) {
			throw e;
		}
		finally {
			pageBuffer = null;
			in = null;
			if (ireader != null) ireader.close();
			ireader = null;
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		jObject.clear();
		super.finalize();
	}
	
	public static String unescapeUnicode(String str) {
		Matcher m = Pattern.compile("\\\\u([0-9a-fA-F]{4})").matcher(str);
		while (m.find()) {
			str = str.replaceFirst("\\\\u([0-9a-fA-F]{4})", new StringBuffer().append((char) Integer.parseInt(m.group(1), 16)).toString());
		}
		return str;
	}
	
	public void doObject(final JObject jObject) {
		// seeJObject(jObject);
	}
	
	public void process(String json) {
		this.json = json;
		process();
	}
	
	public JSONObject getJson() {
		return jobj;
	}
	
	@SuppressWarnings("rawtypes")
	public void process() {
		Map map = new HashMap();
		JsonConfig jc = new JsonConfig();
		jc.setClassMap(map);
		jc.setRootClass(Map.class);
		jc.setArrayMode(JsonConfig.MODE_LIST);
		jobj = JSONObject.fromObject(json, jc);
		//
		toObject(jobj, jObject);
		doObject(jObject);
		//
		jc = null;
		map.clear();
		map = null;
	}
	
	private void toObject(Object json, JObject obj) {
		if (json instanceof JSONArray) {
			JArray obj2 = new JArray();
			decodeJSONObject((JSONArray) json, obj2);
			obj.put(".", obj2);
		}
		else if (json instanceof JSONObject) {
			JObject obj2 = new JObject();
			decodeJSONObject((JSONObject) json, obj2);
			obj.put(".", obj2);
		}
	}
	
	private JArray decodeJSONObject(JSONArray json, JArray obj) {
		Object o;
		int size = json.size();
		for (int i = 0; i < size; i++) {
			o = json.get(i);
			if (o instanceof JSONObject) {
				JObject obj2 = new JObject();
				obj.add(decodeJSONObject((JSONObject) o, obj2));
			}
			else if (o instanceof JSONArray) {
				JArray obj2 = new JArray();
				obj.add(decodeJSONObject((JSONArray) o, obj2));
			}
			else {
				obj.add(o);
			}
		}
		return obj;
	}
	
	@SuppressWarnings({ "unchecked" })
	private JObject decodeJSONObject(JSONObject json, JObject obj) {
		String key;
		Object o;
		for (Iterator<String> keys = json.keys(); keys.hasNext();) {
			key = keys.next();
			o = json.get(key);
			if (o instanceof JSONObject) {
				JObject obj2 = new JObject();
				obj.put(key, decodeJSONObject((JSONObject) o, obj2));
			}
			else if (o instanceof JSONArray) {
				JArray obj2 = new JArray();
				obj.put(key, decodeJSONObject((JSONArray) o, obj2));
			}
			else {
				obj.put(key, o);
			}
		}
		return obj;
	}
	
	public void seeJObject(Object obj) {
		if (obj instanceof String) {
			System.out.println(obj.toString());
		}
		else {
			seeJObject(obj, 0);
		}
	}
	
	@SuppressWarnings({ "rawtypes" })
	private void seeJObject(Object obj, int deep) {
		if (obj == null) {
			return;
		}
		if (obj instanceof JObject) {
			JObject mapObj = (JObject) obj;
			String blank = "";
			for (int i = 0; i < deep; i++) {
				blank += "    ";
			}
			System.out.print("\n" + blank + "{");
			Iterator iter = mapObj.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				System.out.print((String) entry.getKey() + "=");
				seeJObject(entry.getValue(), deep + 1);
				System.out.print(",");
			}
			System.out.print("}");
		}
		else if (obj instanceof JArray) {
			JArray aryObj = (JArray) obj;
			String blank = "";
			for (int i = 0; i < deep; i++) {
				blank += "    ";
			}
			System.out.print("\n" + blank + "[");
			Iterator iter = aryObj.iterator();
			while (iter.hasNext()) {
				seeJObject(iter.next(), deep + 1);
				System.out.print(", ");
			}
			if (blank.length() > 4) {
				System.out.print("]\n" + blank.substring(0, blank.length() - 4));
			}
			else {
				System.out.print("]\n");
			}
		}
		else {
			System.out.print(obj.toString());
		}
	}
	
	public Object query(String path) {
		return query(jObject, path);
	}
	
	// "./key1/key2/[0]/key3"
	@SuppressWarnings({ "rawtypes" })
	public Object query(JObject obj, String path) {
		try {
			String[] keys = path.split("/");
			Object result = obj;
			//
			for (int i = 0; i < keys.length; i++) {
				if (keys[i].charAt(0) == '[' && keys[i].charAt(keys[i].length() - 1) == ']') {
					int idx = Integer.parseInt(keys[i].substring(1, keys[i].length() - 1));
					if (result instanceof JArray) {
						result = ((JArray) result).get(idx);
					}
					else if (result instanceof JObject) {
						Iterator iter = ((JObject) result).entrySet().iterator();
						Map.Entry entry;
						do {
							entry = (Entry) iter.next();
						} while (iter.hasNext() && idx-- > 0);
						result = entry.getValue();
					}
				}
				else {
					result = ((JObject) result).get(keys[i]);
				}
			}
			return result;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
