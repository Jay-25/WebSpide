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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class WebSpideOutput4House extends CSpideOutput {
	
	private final static String clzName              = "WebSpideOutput4House";
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
	private final static int    MDB_INDEX_DUPLICATE  = 1;
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
	
	public WebSpideOutput4House() {
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
		if (!resource.stop) logger.info("Begin [ " + clzName + " ]" + (resource.test ? "(test)" : ""));
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
					String address = datajson.query("./address").toString().replaceAll("\'", "");
					String tab_name = "";
					String sql_select = "select distinct lower(ct.state_py) py from tab_china_city ct where substr(ct.state,1,2)=substr('" + address
					                .substring(0, Math.min(6, address.length())) + "',1,2) or (strpos(ct.city,'市')>0 and substr(ct.city,1,2)=substr('" + address
					                .substring(0, Math
					                                .min(6, address.length())) + "',1,2))";
					Statement stm = resource.con.createStatement();
					ResultSet rs = stm.executeQuery(sql_select);
					if (rs.next()) {
						tab_name = "_" + rs.getString("py");
					}
					String sql_insert = "INSERT INTO public.tab_second_house" + tab_name + " (release_time, price, price_unit, house_property, house_style, house_struct, house_decoration, house_class, build_class, usage_area, " + "build_area, build_name, build_time_year, build_face, build_layer, build_max_layer, address_city, address, develop_company, property_costs, " + "property_company, mortgage_down_payment, monthly, url, url_cofrom, web_in_uid, postcode, coordinate, elevation, cofrom) VALUES " + "(?::TIMESTAMP, to_number(?,'999999.999'), to_number(?,'999999.999'), ?, ?, ?, ?, ?, ?, to_number(?,'999999.999')," + " to_number(?,'999999.999'), ?, to_number(?,'9999'), ?, to_number(?,'99999'), to_number(?,'99999'), ?, ?, ?, ?, " + "?, to_number(?,'999999.999'), to_number(?,'999999.999'), ?, ?, ?, ?, POINT(to_number(?,'9999.9999999'),to_number(?,'9999.9999999')), to_number(?,'9999.9999999'), ?)";
					PreparedStatement st = resource.con.prepareStatement(sql_insert);
					st.setString(1, datajson.query("./release_time").toString());
					st.setString(2, decode(datajson.query("./price").toString(), "0"));
					st.setString(3, decode(datajson.query("./price_unit").toString(), "0"));
					st.setString(4, datajson.query("./house_property").toString());
					st.setString(5, datajson.query("./house_style").toString());
					st.setString(6, datajson.query("./house_struct").toString());
					st.setString(7, datajson.query("./house_decoration").toString());
					st.setString(8, datajson.query("./house_class").toString());
					st.setString(9, datajson.query("./build_class").toString());
					st.setString(10, decode(datajson.query("./usage_area").toString(), "0"));
					st.setString(11, decode(datajson.query("./build_area").toString(), "0"));
					st.setString(12, datajson.query("./build_name").toString());
					st.setString(13, decode(datajson.query("./build_time_year").toString(), "0"));
					st.setString(14, datajson.query("./build_face").toString());
					st.setString(15, decode(datajson.query("./build_layer").toString(), "0"));
					st.setString(16, decode(datajson.query("./build_max_layer").toString(), "0"));
					st.setString(17, datajson.query("./address_city").toString());
					st.setString(18, datajson.query("./address").toString());
					st.setString(19, datajson.query("./develop_company").toString());
					st.setString(20, datajson.query("./property_costs").toString());
					st.setString(21, datajson.query("./property_company").toString());
					st.setString(22, decode(datajson.query("./mortgage_down_payment").toString(), "0"));
					st.setString(23, decode(datajson.query("./monthly").toString(), "0"));
					st.setString(24, datajson.query("./url").toString());
					st.setString(25, datajson.query("./url_cofrom").toString());
					st.setString(26, datajson.query("./web_in_uid").toString());
					st.setString(27, "");
					st.setString(28, decode(datajson.query("./longitude").toString(), "0"));
					st.setString(29, decode(datajson.query("./latitude").toString(), "0"));
					st.setString(30, decode(datajson.query("./elevation").toString(), "0"));
					st.setString(31, datajson.query("./style").toString());
					st.executeUpdate();
					//
					st.close();
					st = null;
					rs.close();
					rs = null;
					stm.close();
					stm = null;
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
					String key = datajson.query("./url_cofrom").toString();
					int dup = 1;
					if (outputQueue.jedisExists(MDB_INDEX_DUPLICATE, key)) {
						dup += Integer.parseInt(outputQueue.jedisGet(MDB_INDEX_DUPLICATE, key));
					}
					outputQueue.jedisSet(MDB_INDEX_DUPLICATE, key, "" + dup);
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
			String path = resource.dataPath + File.separator + "House";
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
			resource.outputQueue.jedisSet(CJobQueue.MDB_INDEX_SERVER, key_Outputer_Running, "0");
		}
		else {
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
							WebSpideOutput4House.Status status = WebSpideOutput4House.Status.STATUS_OK;
							String strJson = resource.outputQueue
							                .getData(WebSpideOutput4House.QUEUE_INDEX_OUTPUT);
							if (strJson == null) continue;
							status = insert(resource.outputQueue, strJson, finalTest);
							if (status == WebSpideOutput4House.Status.STATUS_OK) {
								numOk++;
							}
							else if (status == WebSpideOutput4House.Status.STATUS_EXIST) {
								numExit++;
							}
							else if (status == WebSpideOutput4House.Status.STATUS_ERROR) {
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
		new WebSpideOutput4House().output(args);
	}
}
