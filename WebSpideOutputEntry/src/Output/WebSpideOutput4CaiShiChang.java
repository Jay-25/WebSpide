package Output;

// jconsole
/*
 * -Xverify:none -Xms1024M -Xmx1024M -Xmn600M -XX:PermSize=96M -XX:MaxPermSize=96M -Xss1M -XX:ParallelGCThreads=2 -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:CMSFullGCsBeforeCompaction=5
 * -XX:CMSInitiatingOccupancyFraction=85 -XX:MaxTenuringThreshold=0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
 */
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
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
import SpiderBase.CSpideOutput;

public class WebSpideOutput4CaiShiChang extends CSpideOutput {
	
	private final static String clzName              = "WebSpideOutput4CaiShiChang";
	//
	private final static Logger logger;
	static {
		CLog.setLogger(clzName);
		logger = CLog.getLogger();
	}
	//
	private static boolean      isPrint              = true;
	//
	private final static int    QUEUE_INDEX_OUTPUT   = 0;
	private final String        key_Outputer_Running = "Outputer-Running-" + clzName;
	
	private static enum Status {
		STATUS_OK, STATUS_ERROR, STATUS_EXIST
	}
	
	private class Resource {
		
		private String          iniFileName    = "";
		private boolean         test           = false;
		private boolean         stop           = false;
		private String          dataPath       = null;
		//
		private String          driver         = null;
		private String          url            = null;
		private String          dbname         = null;
		private String          user           = null;
		private String          password       = null;
		//
		private Connection      con            = null;
		private CJobQueueConfig jobQueueConfig = null;
		private CJobQueue       outputQueue    = null;
	}
	
	private Resource resource = new Resource();
	
	public WebSpideOutput4CaiShiChang() {
	}
	
	@Override
	public String getName() {
		return clzName;
	}
	
	private boolean init(String[] args) throws InstantiationException, IllegalAccessException,
	                ClassNotFoundException, SQLException {
		if (args.length <= 0) {
			CSpideVersion.printVersion(clzName);
			System.out.println("java -jar " + clzName + ".jar <-c inifile>");
			System.out.println("option:");
			System.out.println("       -c <ini file> : config file.");
			System.out.println("       -test         : for test.");
			System.out.println("       -stop         : stop server.");
			System.out.println("       -quiet        : no print.");
			return false;
		}
		//
		resource.test = false;
		resource.stop = false;
		//
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-c")) {
				resource.iniFileName = args[i + 1];
				i++;
			}
			else if (args[i].equals("-test")) {
				resource.test = true;
			}
			else if (args[i].equals("-stop")) {
				resource.stop = true;
			}
			else if (args[i].equals("-quiet")) {
				isPrint = false;
			}
		}
		//
		if (resource.con == null && resource.outputQueue == null) {
			File iniFile = new File(resource.iniFileName);
			IniFile ini = new BasicIniFile(false);// 大小写不敏感
			IniFileReader reader = new IniFileReader(ini, iniFile);
			try {
				reader.read();
				for (int i = 0; i < ini.getNumberOfSections(); i++) {
					IniSection sec = ini.getSection(i);
					if (sec.getName().equals("DB")) {
						resource.driver = sec.getItem("driver").getValue();
						resource.url = sec.getItem("url").getValue();
						resource.dbname = sec.getItem("dbname").getValue();
						resource.user = sec.getItem("user").getValue();
						resource.password = sec.getItem("password").getValue();
					}
					else if (sec.getName().equals("Data")) {
						resource.dataPath = sec.getItem("path").getValue();
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
			//
			resource.jobQueueConfig = new CJobQueueConfig(resource.iniFileName);
			resource.outputQueue = new CJobQueue(resource.jobQueueConfig);
			resource.outputQueue.setQueueName(resource.jobQueueConfig.getQueueName() + "-OUTPUT");
		}
		//
		return true;
	}
	
	private void openDB() throws InstantiationException, IllegalAccessException,
	                ClassNotFoundException, SQLException {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Class.forName(resource.driver).newInstance();
				resource.con = DriverManager.getConnection(resource.url + resource.dbname, resource.user, resource.password);
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				try {
					Thread.sleep(10 * 1000);
				}
				catch (InterruptedException e1) {
				}
			}
		}
	}
	
	private void closeDB() {
		try {
			resource.con.close();
		}
		catch (Exception e) {
		}
		resource.con = null;
	}
	
	@Override
	protected void finalize() throws Throwable {
		closeDB();
		resource = null;
		super.finalize();
	}
	
	private String decode(String v1, String v2) {
		if (v1 == null || v1.length() <= 0 || v1.equals("null")) return v2;
		return v1;
	}
	
	private Status insert(final CJobQueue outputQueue, String strJson, boolean test) {
		CJson datajson = new CJson(strJson);
		datajson.process();
		if (!datajson.isValid()) return Status.STATUS_ERROR;
		int retry = 5;
		while (retry > 0) {
			try {
				if (!test) {
					String dataStyle = datajson.query("./dataStyle").toString();
					if (dataStyle.equals("0")) {
						String sql_insert = "INSERT INTO tab_caishichang_price(updateTime,market,type,name,avgPrice,maxPrice,minPrice,average) VALUES (?::TIMESTAMP,?,?,?,to_number(?,'999999.999'),to_number(?,'999999.999'),to_number(?,'999999.999'),?)";
						//
						PreparedStatement st = resource.con.prepareStatement(sql_insert);
						st.setString(1, datajson.query("./updateTime").toString());
						st.setString(2, decode(datajson.query("./market").toString().trim(), ""));
						st.setString(3, decode(datajson.query("./type").toString().trim(), ""));
						st.setString(4, decode(datajson.query("./name").toString().trim(), ""));
						st.setString(5, decode(datajson.query("./avgPrice").toString(), "0"));
						st.setString(6, decode(datajson.query("./maxPrice").toString(), "0"));
						st.setString(7, decode(datajson.query("./minPrice").toString(), "0"));
						st.setBoolean(8, datajson.query("./average").toString().equals("1"));
						st.executeUpdate();
						//
						st.close();
						st = null;
					}
					else if (dataStyle.equals("1")) {
						String sql_insert = "INSERT INTO tab_caishichang_nutrition(food,name,unit,value,nrv,per) VALUES (?,?,?,to_number(?,'999999.999'),to_number(?,'999999.999'),?)";
						//
						PreparedStatement st = resource.con.prepareStatement(sql_insert);
						st.setString(1, datajson.query("./food").toString());
						st.setString(2, decode(datajson.query("./name").toString().trim(), ""));
						st.setString(3, decode(datajson.query("./unit").toString().trim(), ""));
						st.setString(4, decode(datajson.query("./val").toString().trim(), "0"));
						st.setString(5, decode(datajson.query("./nrv").toString(), "0"));
						st.setString(6, decode(datajson.query("./per").toString(), ""));
						st.executeUpdate();
						//
						st.close();
						st = null;
					}
					else if (dataStyle.equals("2")) {
						String sql_insert = "INSERT INTO tab_caishichang_market(district, type, name, updated) VALUES (?,?,?,?)";
						//
						PreparedStatement st = resource.con.prepareStatement(sql_insert);
						st.setString(1, datajson.query("./district").toString());
						st.setString(2, decode(datajson.query("./type").toString().trim(), ""));
						st.setString(3, decode(datajson.query("./name").toString().trim(), ""));
						st.setBoolean(4, datajson.query("./updated").toString().equals("1"));
						st.executeUpdate();
						//
						st.close();
						st = null;
					}
					else {
					}
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
	
	private void saveFile(String data) {
		try {
			CJson datajson = new CJson(data);
			datajson.process();
			//
			String path = resource.dataPath + File.separator + "CaiJia";
			File dir = new File(path);
			if (!dir.isDirectory()) {
				dir.mkdirs();
			}
			dir = null;
			//
			DateFormat format2 = new java.text.SimpleDateFormat("yyyyMMdd");
			String fileName = format2.format(new Date());
			File file = new File(path + File.separator + datajson.query("./job_name") + "_" + fileName + ".data");
			FileWriter fw = null;
			try {
				fw = new FileWriter(file, true);
				fw.write(data);
				fw.write("\n");
				fw.flush();
			}
			catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			finally {
				try {
					fw.close();
				}
				catch (IOException e) {
				}
				fw = null;
				datajson = null;
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	@Override
	public void output(String[] args) {
		try {
			if (!init(args)) {
				return;
			}
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			return;
		}
		//
		if (resource.stop) {
			logger.info("Stop [ " + clzName + " ]");
			resource.outputQueue.jedisSet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running, "0");
		}
		else {
			logger.info("Begin [ " + clzName + " ]" + (resource.test ? "(test)" : ""));
			final boolean finalTest = resource.test;
			new Thread(new Runnable() {
				
				@Override
				public void run() {
					String timestamp = null;
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-M-dd HH:mm:ss");
					long numOk = 0;
					long numExit = 0;
					long numError = 0;
					long showCounter = 0;
					resource.outputQueue.jedisSet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running, "1");
					while (resource.outputQueue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running).equals("1")) {
						while (resource.outputQueue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running).equals("1")
						                && resource.outputQueue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0) {
							try {
								if (!isPrint && ++showCounter % 100 == 0) {
									Date now = new Date();
									timestamp = dateFormat.format(now);
									now = null;
									System.out.println("--- < " + clzName + ", " + timestamp + ", OK: " + numOk + ", Exist: " + numExit + ", Error: " + numError + " > ---");
								}
								Thread.sleep(50);
							}
							catch (InterruptedException e) {
							}
						}
						if (!resource.outputQueue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running).equals("1")) {
							logger.info("End [ " + clzName + " ]");
							return;
						}
						//
						try {
							WebSpideOutput4CaiShiChang.Status status = WebSpideOutput4CaiShiChang.Status.STATUS_OK;
							String strJson = resource.outputQueue
							                .getData(WebSpideOutput4CaiShiChang.QUEUE_INDEX_OUTPUT);
							if (strJson == null) continue;
							status = insert(resource.outputQueue, strJson, finalTest);
							if (status == WebSpideOutput4CaiShiChang.Status.STATUS_OK) {
								numOk++;
							}
							else if (status == WebSpideOutput4CaiShiChang.Status.STATUS_EXIST) {
								numExit++;
							}
							else if (status == WebSpideOutput4CaiShiChang.Status.STATUS_ERROR) {
								numError++;
								saveFile(strJson);
							}
						}
						catch (Exception e) {
							logger.warn(e.getMessage(), e);
							return;
						}
					}
					logger.info("End [ " + clzName + " ]");
				}
			}, "Trd-" + clzName + "-main").start();
		}
	}
	
	public static void main(String[] args) {
		new WebSpideOutput4CaiShiChang().output(args);
	}
}
