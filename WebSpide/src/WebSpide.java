// jconsole
/*
 * -Xverify:none -Xms1024M -Xmx1024M -Xmn600M -XX:PermSize=96M -XX:MaxPermSize=96M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:CMSFullGCsBeforeCompaction=5
 * -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
 */
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;

import redis.clients.jedis.Jedis;
import Algorithm.Segment.dic.CSegment;
import ClassLoader.CClassLoader;
import Job.CJobQueue;
import Job.CJobQueueConfig;
import Job.CJobService4Worker;
import Job.CJobService4WorkerConfig;
import Job.CSpideVersion;
import Log.CLog;
import SpiderBase.SpideEntryBase;
import SpiderJob.CSpideJob;

public class WebSpide {
	
	private final static Logger logger;
	static {
		CLog.setLogger("WebSpideClient");
		logger = CLog.getLogger();
	}
	
	public static void main(String[] args) {
		if (args.length <= 1) {
			CSpideVersion.printVersion("WebSpideClient");
			System.out.println("java -jar WebSpideClient.jar <-c inifile>");
			System.out.println("option:");
			System.out.println("       -c <ini file> : config file.");
			System.out.println("       -dic <path>   : segment dic path.");
			System.out.println("       -once         : once model.");
			return;
		}
		//
		String iniFileName = "";
		boolean once = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				iniFileName = args[i + 1];
				i++;
			}
			else if (args[i].equals("-dic")) {
				CSegment.getInstance().loadCustomDic(args[i + 1]);
				i++;
			}
			else if (args[i].equals("-once")) {
				once = true;
			}
		}
		//
		logger.info("Begin [ WebSpideClient ]");
		//
		try {
			final CJobQueueConfig jobQueueConfig = new CJobQueueConfig(iniFileName);
			final CJobQueue jobQueue = new CJobQueue(jobQueueConfig);
			final CJobService4WorkerConfig jobService4WorkerConfig = new CJobService4WorkerConfig(iniFileName);
			final CJobService4Worker jobService4Worker = new CJobService4Worker(jobQueue, jobService4WorkerConfig, new CSpideJob() {
				
				@Override
				protected boolean execute(String path, String jobname, String url, ArrayList<Object> paras) {
					boolean result = true;
					SpideEntryBase job = (SpideEntryBase) CClassLoader.loadInstance(path, jobname);
					if (job != null) {
						Jedis spideLogEdis = jobQueue.getJedis(CJobQueue.MDB_INDEX_LOG);
						result = job.run(jobService4WorkerConfig, spideLogEdis, path, jobname, url, paras);
						spideLogEdis.close();
						job = null;
					}
					else {
						logger.error("Can't load class " + jobname + " from [" + path + "]");
					}
					return result;
				}
			});
			jobService4Worker.run(once);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
}
