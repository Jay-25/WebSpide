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
	
	public Jedis getJedis(int queueIdx) {
		int retry = 3;
		while (retry-- > 0) {
			try {
				Jedis jedis = pool.getResource();
				jedis.select(queueIdx);
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
		Jedis jedis = pool.getResource();
		jedis.flushAll();
		jedis.close();
	}
	
	public void empty(int queueIdx) {
		Jedis jedis = getJedis(queueIdx);
		jedis.flushDB();
		jedis.close();
	}
	
	public long length(int queueIdx) {
		Jedis jedis = getJedis(queueIdx);
		long len = jedis.llen(queueName);
		jedis.close();
		return len;
	}
	
	public void addJob(int queueIdx, String... jobs) {
		Jedis jedis = getJedis(queueIdx);
		jedis.rpush(queueName, jobs);
		jedis.close();
	}
	
	public String getJob(int queueIdx) {
		Jedis jedis = getJedis(queueIdx);
		String str = jedis.lpop(queueName);
		jedis.close();
		return str;
	}
}
