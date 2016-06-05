/**
 * @Title: CJobService4Server.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-17 上午11:09:19
 * @version V1.0
 */
package Job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.apache.logging.log4j.Logger;

import redis.clients.jedis.Jedis;
import Log.CLog;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job.CJobService4Server
 * @author：Zhao Jietong
 * @Create：2016-5-17 上午11:09:19
 * @version V1.0
 */
public class CJobService4Server {
	
	private static Logger            logger             = CLog.getLogger();
	private final String             key_Server_Running = "Server-Running";
	private CJobQueue                jobQueue           = null;
	private CJobService4ServerConfig config             = null;
	private boolean                  isOnceModel        = false;
	
	public CJobService4Server(CJobQueue jobQueue, CJobService4ServerConfig config) {
		this.jobQueue = jobQueue;
		this.config = config;
	}
	
	public void ignoreSpided() {
		jobQueue.empty(CJobQueue.MDB_INDEX_LOG);
	}
	
	public void setOnceModel(boolean isOnceModel) {
		this.isOnceModel = isOnceModel;
	}
	
	public void run(boolean force) {
		if (force) {
			jobQueue.empty(CJobQueue.QUEUE_INDEX_JOB);
			jobQueue.empty(CJobQueue.QUEUE_INDEX_RESULT);
			jobQueue.empty(CJobQueue.QUEUE_INDEX_FAIL);
			jobQueue.empty(CJobQueue.MDB_INDEX_RUNNING);
		}
		//
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				logger.info("--- Server Running ---");
				Jedis jedis = jobQueue.getJedis(CJobQueue.MDB_INDEX_SERVER);
				jedis.set(key_Server_Running, "1");
				FilenameFilter filenameFilter = new FilenameFilter() {
					
					@Override
					public boolean accept(File dir, String name) {
						return name.endsWith(config.getJobFileExName());
					}
				};
				while (jedis.get(key_Server_Running).equals("1")) {
					while (jedis.get(key_Server_Running).equals("1") && jobQueue.length(CJobQueue.QUEUE_INDEX_JOB) > 0) {
						sleep(50);
					}
					if (!jedis.get(key_Server_Running).equals("1")) return;
					//
					logger.info("--- Job Loading ---");
					ArrayList<String> jobList = new ArrayList<String>();
					if (jobQueue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0) {
						File dir = new File(config.getJobFilePath());
						if (!dir.exists()) {
							logger.error(config.getJobFilePath() + " NOT Exist!");
						}
						File[] filesOrDirs = dir.listFiles(filenameFilter);
						for (int i = 0; i < filesOrDirs.length; i++) {
							if (filesOrDirs[i].isFile()) {
								logger.info("Loading JobFile: " + filesOrDirs[i]);
								ArrayList<String> _jobList = getJobListFromFile(filesOrDirs[i]);
								for (String str : _jobList) {
									jobList.add(str);
									logger.info("    Load Job: " + str);
								}
								_jobList = null;
							}
						}
						dir = null;
						filesOrDirs = null;
					}
					while (jobQueue.length(CJobQueue.QUEUE_INDEX_FAIL) > 0) {
						String failJobStr = jobQueue.getJob(CJobQueue.QUEUE_INDEX_FAIL);
						for (int i = 0; i < jobList.size(); i++) {
							if (jobList.get(i).equals(failJobStr)) {
								jobList.remove(i);
								break;
							}
						}
					}
					String[] jobArray = new String[jobList.size()];
					jobQueue.addJob(CJobQueue.QUEUE_INDEX_JOB, jobList.toArray(jobArray));
					jobArray = null;
					jobList = null;
					System.out.println("Jobs Number: " + jobQueue.length(CJobQueue.QUEUE_INDEX_JOB));
					System.out.println();
					//
					if (isOnceModel) return;
				}
				jedis.close();
				filenameFilter = null;
			}
		}, "Trd-" + getClass().getName() + "-run").start();
	}
	
	public void stop(boolean toEmpty) {
		Jedis jedis = jobQueue.getJedis(CJobQueue.MDB_INDEX_SERVER);
		jedis.set(key_Server_Running, "0");
		if (toEmpty) {
			jobQueue.empty(CJobQueue.QUEUE_INDEX_JOB);
			jobQueue.empty(CJobQueue.QUEUE_INDEX_RESULT);
			jobQueue.empty(CJobQueue.QUEUE_INDEX_FAIL);
		}
		jedis.close();
	}
	
	private ArrayList<String> getJobListFromFile(File file) {
		ArrayList<String> jobList = new ArrayList<String>();
		String path = "";
		Pattern pline = Pattern.compile("((.+?\\))|(\\S+))\\s+(\\S+)");
		Pattern pspide = Pattern.compile("([^()]*)\\((.*)\\)");
		Pattern pbreak = Pattern.compile("^[ ]*break\\W*$");
		Pattern ppath = Pattern.compile("^[ ]*path[ ]*=[ ]*(.*)");
		InputStream fis = null;
		InputStreamReader reader = null;
		BufferedReader br = null;
		try {
			String line = null;
			fis = new FileInputStream(file);
			reader = new InputStreamReader(fis, "UTF-8");
			br = new BufferedReader(reader);
			for (int timeout = 300; timeout > 0 && !br.ready(); timeout--) {
				Thread.sleep(10);
			}
			JSONObject json = new JSONObject();
			while ((line = br.readLine()) != null) {
				line = line.trim();
				//
				Matcher mpath = ppath.matcher(line);
				if (mpath.find()) {
					path = mpath.group(1).trim();
					continue;
				}
				//
				if (pbreak.matcher(line).find()) {
					break;
				}
				//
				if (line.length() < 8 || line.getBytes()[0] == '#') {
					continue;
				}
				//
				Matcher m = pline.matcher(line);
				if (m.find()) {
					String classname = m.group(1).trim();
					String paras = "";
					Matcher m2 = pspide.matcher(classname);
					if (m2.find()) {
						classname = m2.group(1).trim();
						paras = m2.group(2).trim();
					}
					String url = m.group(4).trim();
					//
					json.clear();
					json.put("path", path);
					json.put("job", classname);
					json.put("paras", paras.split(","));
					json.put("url", url);
					jobList.add(json.toString());
				}
			}
			json = null;
		}
		catch (Exception e) {
		}
		finally {
			try {
				if (reader != null) reader.close();
				reader = null;
				if (br != null) br.close();
				br = null;
				if (fis != null) fis.close();
				fis = null;
			}
			catch (IOException e) {
			}
		}
		return jobList;
	}
	
	private void sleep(int ms) {
		try {
			Thread.sleep(ms);
		}
		catch (InterruptedException e) {
		}
	}
}
