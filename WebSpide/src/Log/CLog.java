/**
 * @Title: CLog.java
 * @Package AppSync
 * @Description: TODO
 * @author
 * @date 2015-11-26 上午9:40:11
 * @version V1.0
 */
package Log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @Copyright：2015
 * @Project：WebServiceSync2
 * @Description：
 * @Class：AppSync.CLog
 * @author：Zhao Jietong
 * @Create：2015-11-26 上午9:40:11
 * @version V1.0
 */
public class CLog {
	
	private static Logger logger = null;
	
	public static void setLogger(String name) {
		try {
			String config = System.getProperty("user.dir") + File.separator + name + "-Log4j2.xml";
			if (!new File(config).exists()) {
				try {
					FileWriter writer = new FileWriter(config, false);
					writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
					                "<!-- -Dlog4j.configurationFile=log4j2.xml -->\n" +
					                "<Configuration status=\"OFF\">\n" +
					                "	<properties>\n" +
					                "		<property name=\"LOG_HOME\">" + System.getProperty("user.dir") + File.separator + "log</property>\n" +
					                "	</properties>\n" +
					                "	<Appenders>\n" +
					                "		<Console name=\"Console\" target=\"SYSTEM_OUT\">\n" +
					                "			<PatternLayout pattern=\"%msg%n\" />\n" +
					                "		</Console>\n" +
					                "		<RollingFile name=\"RollingFile\" fileName=\"${LOG_HOME}/" + name + "-Logs.log\"" + " filePattern=\"${LOG_HOME}/$${date:yyyyMMdd}/%d{yyyyMMdd}_%i.log\" Append=\"true\">\n" +
					                "			<PatternLayout pattern=\"%date{yyyy-MM-dd HH:mm:ss.SSS} %level [%thread][%file:%line] - %msg%n\" />\n" +
					                "			<Policies>\n" +
					                "				<TimeBasedTriggeringPolicy interval=\"1\" modulate=\"true\" />\n" +
					                "				<SizeBasedTriggeringPolicy size=\"16 MB\" />\n" +
					                "			</Policies>\n" +
					                "			<DefaultRolloverStrategy max=\"999\" />\n" +
					                "		</RollingFile>\n" +
					                "	</Appenders>\n" +
					                "	<Loggers>\n" +
					                "		<Root level=\"info\"> <!-- trace/debug/info/warn/error/fatal -->\n" +
					                "			<AppenderRef ref=\"Console\" /> <!-- 在控制台输出 -->\n" +
					                "			<AppenderRef ref=\"RollingFile\" />\n" +
					                "		</Root>\n" +
					                "	</Loggers>\n" +
					                "</Configuration>\n");
					writer.flush();
					writer.close();
					writer = null;
				}
				catch (Exception e) {
				}
			}
			ConfigurationSource source = new ConfigurationSource(new FileInputStream(config));
			Configurator.initialize(null, source);
			source = null;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//
		logger = LogManager.getLogger(name);
	}
	
	public static Logger getLogger() {
		return logger;
	}
}
