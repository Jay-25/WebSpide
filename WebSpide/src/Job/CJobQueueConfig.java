/**
 * @Title: CJobQueue.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午3:34:20
 * @version V1.0
 */
package Job;

import java.io.File;

import org.apache.logging.log4j.Logger;
import org.dtools.ini.BasicIniFile;
import org.dtools.ini.IniFile;
import org.dtools.ini.IniFileReader;
import org.dtools.ini.IniSection;

import redis.clients.jedis.JedisPoolConfig;
import Log.CLog;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job.CJobQueue
 * @author：Zhao Jietong
 * @Create：2016-5-11 下午3:34:20
 * @version V1.0
 */
public class CJobQueueConfig {
	
	private static Logger   logger          = CLog.getLogger();
	private JedisPoolConfig jedisPoolConfig = null;
	private String          queueName       = "Q-WebSpide";
	private String          redisIP         = "localhost";
	private int             redisPort       = 6379;
	private int             timeout         = 10 * 1000;
	
	public CJobQueueConfig() {
		jedisPoolConfig = new JedisPoolConfig();
		// 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
		// jedisPoolConfig.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
		jedisPoolConfig.setJmxNamePrefix("pool");
		// 是否启用后进先出, 默认true
		jedisPoolConfig.setLifo(true);
		// 最大连接数
		jedisPoolConfig.setMaxTotal(500);
		// 最大空闲连接数
		jedisPoolConfig.setMaxIdle(100);
		// 最小空闲连接数, 默认0
		jedisPoolConfig.setMinIdle(0);
		// 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间, 默认-1
		jedisPoolConfig.setMaxWaitMillis(30 * 1000);
		// 对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断 (默认逐出策略)
		jedisPoolConfig.setMinEvictableIdleTimeMillis(1800000);
		// 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
		jedisPoolConfig.setTimeBetweenEvictionRunsMillis(5 * 1000);
		// 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
		jedisPoolConfig.setBlockWhenExhausted(true);
		// 是否启用pool的jmx管理功能, 默认true
		jedisPoolConfig.setJmxEnabled(true);
		// 在空闲时检查有效性, 默认false
		jedisPoolConfig.setTestWhileIdle(true);
		// 出借测试
		jedisPoolConfig.setTestOnBorrow(false);
		// 归还测试
		jedisPoolConfig.setTestOnReturn(false);
	}
	
	public CJobQueueConfig(String configFile) {
		this();
		//
		File iniFile = new File(configFile);
		IniFile ini = new BasicIniFile(false);// 大小写不敏感
		IniFileReader reader = new IniFileReader(ini, iniFile);
		try {
			reader.read();
			for (int i = 0; i < ini.getNumberOfSections(); i++) {
				IniSection sec = ini.getSection(i);
				if (sec.getName().equals("QUEUE")) {
					queueName = sec.getItem("queueName").getValue();
					redisIP = sec.getItem("queueIP").getValue();
					redisPort = Integer.parseInt(sec.getItem("queuePort").getValue());
					timeout = Integer.parseInt(sec.getItem("queueTimeout").getValue());
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
	 * @return the queueName
	 */
	public String getQueueName() {
		return queueName;
	}
	
	/**
	 * @param queueName
	 *            the queueName to set
	 */
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
	public String getRedisIP() {
		return this.redisIP;
	}
	
	public void setRedisIP(String redisIP) {
		this.redisIP = redisIP;
	}
	
	public int getRedisPort() {
		return this.redisPort;
	}
	
	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}
	
	public int getTimeout() {
		return this.timeout;
	}
	
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
	
	public JedisPoolConfig getJedisPoolConfig() {
		return this.jedisPoolConfig;
	}
}
