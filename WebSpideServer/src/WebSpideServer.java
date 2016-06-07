// jconsole
/*
 * -Xverify:none -Xms1024M -Xmx1024M -Xmn600M -XX:PermSize=96M -XX:MaxPermSize=96M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:CMSFullGCsBeforeCompaction=5
 * -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
 */
import org.apache.logging.log4j.Logger;

import Job.CJobQueue;
import Job.CJobQueueConfig;
import Job.CJobService4Server;
import Job.CJobService4ServerConfig;
import Job.CSpideVersion;
import Log.CLog;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：.WebSpideServer
 * @author：Zhao Jietong
 * @Create：2016-5-17 下午4:36:17
 * @version V1.0
 */
public class WebSpideServer {
	
	private final static Logger logger;
	static {
		CLog.setLogger("WebSpideServer");
		logger = CLog.getLogger();
	}
	
	/**
	 * @Title: main
	 * @Description: TODO
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length <= 0) {
			CSpideVersion.printVersion("WebSpideServer");
			System.out.println("java -jar WebSpideServer.jar <-c inifile> [option]");
			System.out.println("option:");
			System.out.println("       -c <ini file> : config file.");
			System.out.println("       -keep         : keep on queue.");
			System.out.println("       -force        : force to ignore spided and restart.");
			System.out.println("       -stop         : stop server.");
			System.out.println("       -once         : once model.");
			return;
		}
		//
		String iniFileName = "";
		boolean keep = false;
		boolean force = false;
		boolean stop = false;
		boolean once = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				iniFileName = args[i + 1];
				i++;
			}
			else if (args[i].equals("-keep")) {
				keep = true;
			}
			else if (args[i].equals("-force")) {
				force = true;
			}
			else if (args[i].equals("-stop")) {
				stop = true;
			}
			else if (args[i].equals("-once")) {
				once = true;
			}
		}
		logger.info("WebSpideServer" + (keep ? "(keep)" : "") + (force ? "(force)" : ""));
		//
		CJobQueueConfig jobQueueConfig = new CJobQueueConfig(iniFileName);
		CJobQueue jobQueue = new CJobQueue(jobQueueConfig);
		CJobService4ServerConfig jobService4ServerConfig = new CJobService4ServerConfig(iniFileName);
		CJobService4Server jobService4Server = new CJobService4Server(jobQueue, jobService4ServerConfig);
		if (force) {
			jobService4Server.ignoreSpided();
		}
		if (stop) {
			jobService4Server.stop(!keep);
		}
		else {
			jobService4Server.setOnceModel(once);
			jobService4Server.run(!keep);
		}
	}
}
