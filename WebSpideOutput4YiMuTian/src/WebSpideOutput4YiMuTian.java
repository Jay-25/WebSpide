// jconsole
/*
 * -Xverify:none -Xms1024M -Xmx1024M -Xmn600M -XX:PermSize=96M -XX:MaxPermSize=96M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:CMSFullGCsBeforeCompaction=5
 * -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
 */
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.Logger;
import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniSection;

import Extract.Json.CJson;
import Job.CJobQueue;
import Job.CJobQueueConfig;
import Job.CSpideVersion;
import Log.CLog;

public class WebSpideOutput4YiMuTian {
	
	private final static String clzName             = "WebSpideOutput4YiMuTian";
	//
	private final static Logger logger;
	static {
		CLog.setLogger(clzName);
		logger = CLog.getLogger();
	}
	//
	private static boolean      isPrint             = true;
	//
	public final static int     QUEUE_INDEX_OUTPUT  = 0;
	public final static int     MDB_INDEX_DUPLICATE = 1;
	
	public static enum Status {
		STATUS_OK, STATUS_ERROR, STATUS_EXIST
	}
	
	private String     dataPath = null;
	private String     driver   = null;
	private String     url      = null;
	private String     dbname   = null;
	private String     user     = null;
	private String     password = null;
	private Connection con      = null;
	
	public WebSpideOutput4YiMuTian(String configFile) throws InstantiationException,
	                IllegalAccessException, ClassNotFoundException, SQLException {
		File iniFile = new File(configFile);
		IniFile ini = new BasicIniFile(false);// 大小写不敏感
		IniFileReader reader = new IniFileReader(ini, iniFile);
		try {
			reader.read();
			for (int i = 0; i < ini.getNumberOfSections(); i++) {
				IniSection sec = ini.getSection(i);
				if (sec.getName().equals("DB")) {
					driver = sec.getItem("driver").getValue();
					url = sec.getItem("url").getValue();
					dbname = sec.getItem("dbname").getValue();
					user = sec.getItem("user").getValue();
					password = sec.getItem("password").getValue();
				}
				else if (sec.getName().equals("Data")) {
					dataPath = sec.getItem("path").getValue();
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
		openDB();
	}
	
	private void openDB() throws InstantiationException, IllegalAccessException,
	                ClassNotFoundException, SQLException {
		Class.forName(driver).newInstance();
		con = DriverManager.getConnection(url + dbname, user, password);
	}
	
	private void closeDB() {
		try {
			con.close();
		}
		catch (Exception e) {
		}
		con = null;
	}
	
	@Override
	protected void finalize() throws Throwable {
		closeDB();
		super.finalize();
	}
	
	private String decode(String v1, String v2) {
		if (v1 == null || v1.length() <= 0 || v1.equals("null")) return v2;
		return v1;
	}
	
	public Status insert(final CJobQueue outputQueue, String strJson, boolean test) {
		CJson datajson = new CJson(strJson);
		datajson.process();
		if (!datajson.isValid()) return Status.STATUS_ERROR;
		int retry = 5;
		while (retry > 0) {
			try {
				if (!test) {
					String sql_insert = "INSERT INTO public.tab_yimutian (update_time,market,type,name,avgprice,maxprice,minprice,unite) VALUES "
					                + "(?::TIMESTAMP,?,?,?,to_number(?,'999999.999'),to_number(?,'999999.999'),to_number(?,'999999.999'),?)";
					//
					PreparedStatement st = con.prepareStatement(sql_insert);
					st.setString(1, datajson.query("./updateTime").toString());
					st.setString(2, decode(datajson.query("./market").toString().trim(), ""));
					st.setString(3, decode(datajson.query("./type").toString().trim(), ""));
					st.setString(4, decode(datajson.query("./name").toString().trim(), ""));
					st.setString(5, decode(datajson.query("./avgPrice").toString(), "0"));
					st.setString(6, decode(datajson.query("./maxPrice").toString(), "0"));
					st.setString(7, decode(datajson.query("./minPrice").toString(), "0"));
					st.setString(8, decode(datajson.query("./unite").toString(), ""));
					st.executeUpdate();
					//
					st.close();
					st = null;
				}
				//
				if (isPrint) {
					System.out.println(datajson.getJson());
				}
				datajson = null;
				//
				return Status.STATUS_OK;
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("08003")) {
					try {
						closeDB();
						Thread.sleep(5000);
						openDB();
						retry--;
						continue;
					}
					catch (Exception e2) {
						logger.warn(e2.getMessage(), e2);
					}
				}
				else if (!e.getSQLState().equals("23505")) {
					logger.warn(e.getMessage(), e);
					return Status.STATUS_ERROR;
				}
				else {
					return Status.STATUS_EXIST;
				}
			}
		}
		return Status.STATUS_ERROR;
	}
	
	public void saveFile(String data) {
	}
	
	public static void main(String[] args) {
		if (args.length <= 0) {
			CSpideVersion.printVersion(clzName);
			System.out.println("java -jar " + clzName + ".jar <-c inifile>");
			System.out.println("option:");
			System.out.println("       -c <ini file> : config file.");
			System.out.println("       -test         : for test.");
			System.out.println("       -stop         : stop server.");
			System.out.println("       -quiet        : no print.");
			return;
		}
		//
		String iniFileName = "";
		boolean test = false;
		boolean stop = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				iniFileName = args[i + 1];
				i++;
			}
			else if (args[i].equals("-test")) {
				test = true;
			}
			else if (args[i].equals("-stop")) {
				stop = true;
			}
			else if (args[i].equals("-quiet")) {
				isPrint = false;
			}
		}
		logger.info("Begin [ " + clzName + " ]" + (test ? "(test)" : ""));
		//
		final WebSpideOutput4YiMuTian output4House;
		try {
			output4House = new WebSpideOutput4YiMuTian(iniFileName);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return;
		}
		//
		CJobQueueConfig jobQueueConfig = new CJobQueueConfig(iniFileName);
		final CJobQueue outputQueue = new CJobQueue(jobQueueConfig);
		outputQueue.setQueueName(jobQueueConfig.getQueueName() + "-OUTPUT");
		if (stop) {
		}
		else {
			final boolean finalTest = test;
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-M-dd HH:mm:ss");
					long numOk = 0;
					long numExit = 0;
					long numError = 0;
					while (true) {
						while (outputQueue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0) {
							try {
								Thread.sleep(50);
							}
							catch (InterruptedException e) {
							}
						}
						//
						try {
							WebSpideOutput4YiMuTian.Status status = WebSpideOutput4YiMuTian.Status.STATUS_OK;
							String strJson = outputQueue
							                .getJob(WebSpideOutput4YiMuTian.QUEUE_INDEX_OUTPUT);
							if (strJson == null) continue;
							status = output4House.insert(outputQueue, strJson, finalTest);
							if (status == WebSpideOutput4YiMuTian.Status.STATUS_OK) {
								numOk++;
							}
							else if (status == WebSpideOutput4YiMuTian.Status.STATUS_EXIST) {
								numExit++;
							}
							else if (status == WebSpideOutput4YiMuTian.Status.STATUS_ERROR) {
								numError++;
								output4House.saveFile(strJson);
							}
							Date now = new Date();
							String timestamp = dateFormat.format(now);
							now = null;
							if (isPrint) {
								System.out.println("--- < " + timestamp + ", OK: " + numOk + ", Exist: " + numExit + ", Error: " + numError + " > ---");
							}
						}
						catch (Exception e) {
							logger.warn(e.getMessage(), e);
							return;
						}
					}
				}
			}, "Trd-" + clzName + "-main").start();
		}
	}
}
