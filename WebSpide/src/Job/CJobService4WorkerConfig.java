/**
 * @Title: CJobService4WorkerConfig.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午4:45:20
 * @version V1.0
 */
package Job;

import java.io.File;

import org.apache.logging.log4j.Logger;
import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniSection;

import Log.CLog;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job.CJobService4WorkerConfig
 * @author：Zhao Jietong
 * @Create：2016-5-11 下午4:45:20
 * @version V1.0
 */
public class CJobService4WorkerConfig {

	protected Logger logger     = CLog.getLogger();
	private String   configFile = null;
	private int      jobNum     = 1;
	private int      attempt    = 5;
	private int      attemptMS  = 3000;
	private int      timeOut    = 60;

	public CJobService4WorkerConfig() {
	}

	public CJobService4WorkerConfig(String configFile) {
		this.configFile = configFile;
		File iniFile = new File(configFile);
		IniFile ini = new BasicIniFile(false);// 大小写不敏感
		IniFileReader reader = new IniFileReader(ini, iniFile);
		try {
			reader.read();
			for (int i = 0; i < ini.getNumberOfSections(); i++) {
				IniSection sec = ini.getSection(i);
				if (sec.getName().equals("SPIDE")) {
					jobNum = Integer.parseInt(sec.getItem("jobNum").getValue());
					attempt = Integer.parseInt(sec.getItem("attempt").getValue());
					attemptMS = Integer.parseInt(sec.getItem("attemptMS").getValue());
					timeOut = Integer.parseInt(sec.getItem("timeOut").getValue());
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
	}

	public String getConfigFile() {
		return this.configFile;
	}

	/**
	 * @return the jobNum
	 */
	public int getJobNum() {
		return jobNum;
	}

	/**
	 * @param jobNum
	 *            the jobNum to set
	 */
	public void setJobNum(int jobNum) {
		this.jobNum = jobNum < 1 ? 1 : jobNum;
	}

	public int getAttempt() {
		return this.attempt;
	}

	public void setAttempt(int attempt) {
		this.attempt = attempt;
	}

	public int getAttemptMS() {
		return this.attemptMS;
	}

	public void setAttemptMS(int attemptMS) {
		this.attemptMS = attemptMS;
	}

	public int getTimeOut() {
		return this.timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}
}
