import java.io.File;

import org.apache.logging.log4j.Logger;
import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniSection;

import ClassLoader.CClassLoader;
import Job.CJobQueue;
import Job.CJobQueueConfig;
import Job.CSpideVersion;
import Log.CLog;
import SpiderBase.CSpideOutput;

class OutputEntry {
	
	private long           timestamp           = 0;
	private boolean        running             = false;
	private CJobQueue      outputQueue         = null;
	private CSpideOutput   spideOutput         = null;
	private String[]       jobOutputParas      = null;
	private final String[] jobOutputParas4Stop = new String[3];
	
	public OutputEntry(CJobQueue outputQueue, CSpideOutput spideOutput, String[] jobOutputParas) {
		this.outputQueue = outputQueue;
		this.spideOutput = spideOutput;
		this.jobOutputParas = jobOutputParas;
		for (int i = 0; i < 2; i++) {
			jobOutputParas4Stop[i] = jobOutputParas[i];
		}
		jobOutputParas4Stop[2] = "-stop";
	}
	
	public boolean hasData() {
		boolean result = outputQueue.length(CJobQueue.QUEUE_INDEX_JOB) > 0;
		if (result) {
			updateTimestamp();
		}
		return result;
	}
	
	public void start() {
		running = true;
		updateTimestamp();
		spideOutput.output(jobOutputParas);
	}
	
	public void stop() {
		spideOutput.output(jobOutputParas4Stop);
		running = false;
	}
	
	public void updateTimestamp() {
		timestamp = System.currentTimeMillis();
	}
	
	public boolean isTimeout(long ms) {
		return (System.currentTimeMillis() - timestamp) > ms;
	}
	
	public boolean isRunning() {
		return running;
	}
}

public class WebSpideOutput {
	
	private final static String clzName = "WebSpideOutput";
	private final static Logger logger;
	static {
		CLog.setLogger(clzName);
		logger = CLog.getLogger();
	}
	
	public static void main(String[] args) {
		if (args.length <= 0) {
			CSpideVersion.printVersion(clzName);
			System.out.println("java -jar " + clzName + ".jar <option>");
			System.out.println("option:");
			System.out.println("       -c <inifile1,initfile2,...> : config files.");
			System.out.println("       -timeout <second>           : timeout to end wacthing");
			return;
		}
		//
		String[] iniFileNames = null;
		long timeoutMs = 10 * 1000;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				iniFileNames = args[i + 1].split(",");
				i++;
			}
			else if (args[i].equals("-timeout")) {
				timeoutMs = 1000 * Long.parseLong(args[i + 1]);
				i++;
			}
		}
		//
		logger.info("Begin [ " + clzName + " ]");
		//
		int outputQueuesIndex = 0;
		OutputEntry[] outputEntries = new OutputEntry[iniFileNames.length];
		for (String configFile : iniFileNames) {
			try {
				String jobOutputClassName = null;
				String[] jobOutputParas = null;
				//
				File iniFile = new File(configFile);
				IniFile ini = new BasicIniFile(false);// 大小写不敏感
				IniFileReader reader = new IniFileReader(ini, iniFile);
				try {
					reader.read();
					for (int i = 0; i < ini.getNumberOfSections(); i++) {
						IniSection sec = ini.getSection(i);
						if (sec.getName().equals("JOB")) {
							jobOutputClassName = sec.getItem("jobOutputClassName").getValue();
							jobOutputParas = ("-c " + configFile + " " + sec
							                .getItem("jobOutputParas")
							                .getValue()).split(" ");
						}
					}
				}
				catch (Exception e) {
					logger.warn(e.getMessage(), e);
				}
				finally {
					reader = null;
					ini = null;
					iniFile = null;
				}
				//
				CJobQueueConfig jobQueueConfig = new CJobQueueConfig(configFile);
				CJobQueue outputQueue = new CJobQueue(jobQueueConfig);
				outputQueue.setQueueName(jobQueueConfig.getQueueName() + "-OUTPUT");
				//
				File clzFile = new File(jobOutputClassName);
				CSpideOutput spideOutput = (CSpideOutput) CClassLoader
				                .loadInstance(clzFile.getParent(), clzFile.getName());
				outputEntries[outputQueuesIndex] = new OutputEntry(outputQueue, spideOutput, jobOutputParas);
				outputQueuesIndex++;
			}
			catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
		//
		iniFileNames = null;
		//
		logger.info("Watching... [timeout=" + (timeoutMs / 1000) + " sec.]");
		while (true) {
			try {
				for (OutputEntry outputEntry : outputEntries) {
					if (outputEntry.hasData()) {
						if (!outputEntry.isRunning()) outputEntry.start();
					}
					else {
						if (outputEntry.isTimeout(timeoutMs)) {
							if (outputEntry.isRunning()) outputEntry.stop();
						}
					}
				}
				Thread.sleep(10);
			}
			catch (Exception e) {
				logger.error(e.getMessage(), e);
				return;
			}
		}
	}
}
