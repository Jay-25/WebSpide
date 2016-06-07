/**
 * @Title: CJobWorker.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午4:00:14
 * @version V1.0
 */
package Job;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import redis.clients.jedis.Jedis;
import Log.CLog;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job.CJobWorker
 * @author：Zhao Jietong
 * @Create：2016-5-11 下午4:00:14
 * @version V1.0
 */
public class CJobService4Worker {
	
	private static Logger logger = CLog.getLogger();
	
	private class _JobCounter {
		
		private final AtomicLong maxNum       = new AtomicLong(0);
		private final AtomicLong jobNum       = new AtomicLong(0);
		private final AtomicLong lastQueueLen = new AtomicLong(0);
		
		private void init(long jobN, long qeueuN) {
			lastQueueLen.set(qeueuN);
			if (qeueuN <= 0) {
				jobNum.set(1);
				maxNum.set(1);
			}
			else {
				if (jobN < qeueuN) {
					jobNum.set(jobN);
					maxNum.set(jobN);
				}
				else {
					jobNum.set(qeueuN);
					maxNum.set(qeueuN);
				}
			}
		}
		
		private void update(long qeueuN) {
			if (lastQueueLen.get() != qeueuN) {
				lastQueueLen.set(qeueuN);
				if (lastQueueLen.get() < maxNum.get()) {
					maxNum.set(lastQueueLen.get());
				}
			}
		}
		
		private boolean jobIsRunable() {
			return jobNum.get() > 0;
		}
		
		private void decrement() {
			if (jobNum.get() > 0) jobNum.decrementAndGet();
		}
		
		private void increment() {
			if (jobNum.get() < maxNum.get()) {
				jobNum.incrementAndGet();
			}
		}
	}
	
	private final _JobCounter        jobCounter = new _JobCounter();
	private CJobQueue                queue      = null;
	private IJobWorker               worker     = null;
	private CJobService4WorkerConfig config     = null;
	
	public CJobService4Worker(CJobQueue queue, CJobService4WorkerConfig config, IJobWorker worker) {
		this.queue = queue;
		this.worker = worker;
		this.config = config;
		this.queue.empty(CJobQueue.MDB_INDEX_RUNNING);
	}
	
	public void run(final boolean once) {
		jobCounter.init(config.getJobNum(), queue.length(CJobQueue.QUEUE_INDEX_JOB));
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					try {
						while (queue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0 || !jobCounter.jobIsRunable()) {
							sleep(50);
						}
						jobCounter.update(queue.length(CJobQueue.QUEUE_INDEX_JOB));
						//
						jobCounter.decrement();
						runConsole(once);
						//
						if (once) break;
					}
					catch (Exception e) {
						logger.warn(e.getMessage(), e);
					}
				}
			}
		}, "Trd-" + getClass().getName() + "-run").start();
	}
	
	private void runConsole(final boolean once) {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					String jobString = queue.getJob(CJobQueue.QUEUE_INDEX_JOB);
					if (jobString == null) {
						jobCounter.jobNum.incrementAndGet();
						return;
					}
					//
					Jedis jedisRunning = queue.getJedis(CJobQueue.MDB_INDEX_RUNNING);
					if (jedisRunning.exists(jobString)) {
						logger.info("Duplicate running " + jobString);
					}
					else {
						jedisRunning.set(jobString, "1");
						try {
							if (worker.execute(jobString) && !once) {
								queue.addJob(CJobQueue.QUEUE_INDEX_RESULT, jobString);
								logger.info("Job SUCCESS and return QUEUE_INDEX_RESULT : " + jobString);
							}
							else {
								queue.addJob(CJobQueue.QUEUE_INDEX_FAIL, jobString);
								logger.warn("Job FALSE : " + jobString);
							}
						}
						catch (Exception e) {
							logger.warn(e.getMessage(), e);
						}
						jedisRunning.del(jobString);
					}
					jedisRunning.close();
				}
				catch (Exception e) {
					logger.warn(e.getMessage(), e);
				}
				//
				jobCounter.increment();
			}
		}, "Trd-" + getClass().getName() + "-runConsole").start();
	}
	
	public void sleep(long ms) {
		try {
			Thread.sleep(ms);
		}
		catch (Exception e) {
		}
	}
}
