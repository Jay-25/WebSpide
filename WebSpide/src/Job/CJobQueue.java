/**
 * @Title: CJobQueue.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午3:34:20
 * @version V1.0
 */
package Job;

import org.apache.logging.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
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
public class CJobQueue {
	
	private static Logger   logger             = CLog.getLogger();
	public final static int QUEUE_INDEX_JOB    = 0;               // 作业队列
	public final static int QUEUE_INDEX_RESULT = 1;               // 完成的作业队列
	public final static int QUEUE_INDEX_FAIL   = 2;               // 失败的作业队列
	public final static int MDB_INDEX_LOG      = 3;               // 已经完成的进度，那个URL完成了多少页
	public final static int MDB_INDEX_SERVER   = 4;               // Service运行状态
	public final static int MDB_INDEX_RUNNING  = 5;               // 正在运行的作业集合
	//
	private JedisPool       pool               = null;
	private CJobQueueConfig config             = null;
	private String          queueName          = "";
	
	public CJobQueue(CJobQueueConfig config) {
		this.config = config;
		this.queueName = config.getQueueName();
		init();
	}
	
	@Override
	protected void finalize() throws Throwable {
		logger.error("JedisPool Fin [" + queueName + "]");
		pool.close();
		super.finalize();
	}
	
	private void init() {
		logger.info("Init JedisPool [" + queueName + "]");
		if (pool != null && !pool.isClosed()) pool.close();
		pool = new JedisPool(config.getJedisPoolConfig(), config.getRedisIP(), config.getRedisPort());
	}
	
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	
	public String getQueueName() {
		return this.queueName;
	}
	
	public void jedisSet(int jedisIdx, String key, String value) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				jedis.set(key, value);
				jedis.close();
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("jedisSet again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
	}
	
	public String jedisGet(int jedisIdx, String key) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				String value = jedis.get(key);
				jedis.close();
				return value;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("jedisGet again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
		return null;
	}
	
	public void jedisDel(int jedisIdx, String key) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				jedis.del(key);
				jedis.close();
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("jedisDel again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
	}
	
	public boolean jedisExists(int jedisIdx, String key) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				boolean result = jedis.exists(key);
				jedis.close();
				return result;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("jedisExists again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
		return false;
	}
	
	private Jedis getJedis(int jedisIdx) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = pool.getResource();
				jedis.select(jedisIdx);
				return jedis;
			}
			catch (Exception e) {
				logger.error("borrow jedis again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
		logger.error("Jedis Get Error!");
		return null;
	}
	
	public void empty() {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = pool.getResource();
				jedis.flushAll();
				jedis.close();
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("empty() again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
	}
	
	public void empty(int jedisIdx) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				jedis.flushDB();
				jedis.close();
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("empty(" + jedisIdx + ") again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
	}
	
	public long length(int jedisIdx) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				long len = jedis.llen(queueName);
				jedis.close();
				return len;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("length(" + jedisIdx + ") again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
		return -1;
	}
	
	public void addData(int jedisIdx, String... jobs) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				jedis.rpush(queueName, jobs);
				jedis.close();
				break;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("empty(" + jedisIdx + ") again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
	}
	
	public String getData(int jedisIdx) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = getJedis(jedisIdx);
				String str = jedis.lpop(queueName);
				jedis.close();
				return str;
			}
			catch (Exception e) {
				if (retry <= 0) throw (e);
				logger.error("empty(" + jedisIdx + ") again [" + e.getMessage() + "]", e);
				try {
					Thread.sleep(2 * 1000);
				}
				catch (Exception e1) {
				}
				init();
			}
		}
		return null;
	}
}
