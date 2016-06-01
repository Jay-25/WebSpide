/**
 * @Title: CSpideJob.java
 * @Package Job4Spide
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午4:32:24
 * @version V1.0
 */
package SpiderJob;

import java.util.ArrayList;

import Extract.Json.CJson;
import Job.CJobDefaultWorker;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job4Spide.CSpideJob
 * @author：Zhao Jietong
 * @Create：2016-5-11 下午4:32:24
 * @version V1.0
 */
public abstract class CSpideJob extends CJobDefaultWorker {
	
	/*
	 * {job: xxxx, paras:[], url:}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean execute(String... args) {
		CJson argJson = new CJson(args[0]);
		argJson.process();
		String path = argJson.query("./path").toString();
		String jobname = argJson.query("./job").toString();
		String url = argJson.query("./url").toString();
		ArrayList<Object> paras = (ArrayList<Object>) argJson.query("./paras");
		argJson = null;
		return execute(path, jobname, url, paras);
	}
	
	protected abstract boolean execute(String path, String jobname, String url, ArrayList<Object> paras);
}
