/**
 * @Title: CJobService4Server.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-17 上午11:09:19
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
 * @Class：Job.CJobService4Server
 * @author：Zhao Jietong
 * @Create：2016-5-17 上午11:09:19
 * @version V1.0
 */

public class CJobService4ServerConfig {

	private static Logger logger        = CLog.getLogger();

	private String        jobFilePath   = "";
	private String        jobFileExName = "";

	public CJobService4ServerConfig() {

	}

	public CJobService4ServerConfig(String configFile) {
		File iniFile = new File(configFile);
		IniFile ini = new BasicIniFile(false);// 大小写不敏感
		IniFileReader reader = new IniFileReader(ini, iniFile);
		try {
			reader.read();
			for (int i = 0; i < ini.getNumberOfSections(); i++) {
				IniSection sec = ini.getSection(i);
				if (sec.getName().equals("JOB")) {
					jobFilePath = sec.getItem("jobPath").getValue();
					jobFileExName = sec.getItem("jobFileEx").getValue();
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

	/**
	 * @return the jobFilePath
	 */
	public String getJobFilePath() {
		return jobFilePath;
	}

	/**
	 * @param jobFilePath
	 *            the jobFilePath to set
	 */
	public void setJobFilePath(String jobFilePath) {
		this.jobFilePath = jobFilePath;
	}

	/**
	 * @return the jobFileExNameString
	 */
	public String getJobFileExName() {
		return jobFileExName;
	}

	/**
	 * @param jobFileExNameString
	 *            the jobFileExNameString to set
	 */
	public void setJobFileExName(String jobFileExName) {
		this.jobFileExName = jobFileExName;
	}

}
