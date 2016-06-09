import java.io.File;

import org.apache.logging.log4j.Logger;
import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniSection;

import ClassLoader.CClassLoader;
import Job.CSpideVersion;
import Log.CLog;
import SpiderBase.CSpideOutput;

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
			System.out.println("java -jar " + clzName + ".jar <-c inifile1,initfile2,...>");
			System.out.println("option:");
			System.out.println("       -c <inifile1,initfile2,...> : config files.");
			return;
		}
		//
		String[] iniFileNames = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				iniFileNames = args[i + 1].split(",");
				i++;
			}
		}
		//
		for (String configFile : iniFileNames) {
			String jobOutputClassName = null;
			String[] jobOutputParas = null;
			File iniFile = new File(configFile);
			IniFile ini = new BasicIniFile(false);// 大小写不敏感
			IniFileReader reader = new IniFileReader(ini, iniFile);
			try {
				reader.read();
				for (int i = 0; i < ini.getNumberOfSections(); i++) {
					IniSection sec = ini.getSection(i);
					if (sec.getName().equals("JOB")) {
						jobOutputClassName = sec.getItem("jobOutputClassName").getValue();
						jobOutputParas = ("-c " + configFile + " " + sec.getItem("jobOutputParas")
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
			File clzFile = new File(jobOutputClassName);
			CSpideOutput spideOutput = (CSpideOutput) CClassLoader
			                .loadInstance(clzFile.getParent(), clzFile.getName());
			spideOutput.output(jobOutputParas);
		}
	}
}
