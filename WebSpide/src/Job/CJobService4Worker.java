/**
 * @Title: CJobWorker.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午4:00:14
 * @version V1.0
 */
package Job;

import org.apache.logging.log4j.Logger;

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
	
	private final static String      clzName            = "CJobService4Worker";
	private static Logger            logger             = CLog.getLogger();
	private final CJobCounter        jobCounter         = new CJobCounter();
	private CJobQueue                queue              = null;
	private IJobWorker               worker             = null;
	private CJobService4WorkerConfig config             = null;
	private String                   key_Client_Running = null;
	
	public CJobService4Worker(CJobQueue queue, CJobService4WorkerConfig config, IJobWorker worker) {
		this.queue = queue;
		this.worker = worker;
		this.config = config;
		this.queue.empty(CJobQueue.MDB_INDEX_RUNNING);
		this.key_Client_Running = "Client-Running-" + clzName + "-" + queue.getQueueName();
	}
	
	public void stop() {
		queue.jedisSet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running, "0");
	}
	
	public void run(final boolean once) {
		jobCounter.init(config.getJobNum(), queue.length(CJobQueue.QUEUE_INDEX_JOB));
		//
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				boolean waiting4Beg = false;
				queue.jedisSet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running, "1");
				Thread thread = new Thread(new Runnable() {
					
					@Override
					public void run() {
						while (true) {
							sleep(50);
							if (!queue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running).equals("1")) {
								worker.stop();
								break;
							}
						}
					}
				});
				thread.setDaemon(true);
				thread.start();
				//
				while (queue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running).equals("1")) {
					try {
						while (queue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running).equals("1")
						                && (queue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0 || !jobCounter.jobIsRunable())) {
							sleep(50);
							if (waiting4Beg && once && queue.length(CJobQueue.QUEUE_INDEX_JOB) <= 0) return;
						}
						if (!queue.jedisGet(CJobQueue.MDB_INDEX_SERVER, key_Client_Running).equals("1")) {
							logger.info("End [ " + clzName + " ]");
							return;
						}
						waiting4Beg = true;
						jobCounter.update(queue.length(CJobQueue.QUEUE_INDEX_JOB));
						//
						jobCounter.decrement();
						runConsole();
					}
					catch (Exception e) {
						logger.warn(e.getMessage(), e);
					}
				}
			}
		}, "Trd-" + getClass().getName() + "-run").start();
	}
	
	private void runConsole() {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					String jobString = queue.getData(CJobQueue.QUEUE_INDEX_JOB);
					if (jobString == null) {
						jobCounter.increment();
						return;
					}
					//
					if (queue.jedisExists(CJobQueue.MDB_INDEX_RUNNING, jobString)) {
						logger.info("Duplicate running " + jobString);
					}
					else {
						queue.jedisSet(CJobQueue.MDB_INDEX_RUNNING, jobString, "1");
						try {
							if (worker.execute(jobString)) {
								queue.addData(CJobQueue.QUEUE_INDEX_RESULT, jobString);
								logger.info("Job SUCCESS and return QUEUE_INDEX_RESULT : " + jobString);
							}
							else {
								queue.addData(CJobQueue.QUEUE_INDEX_FAIL, jobString);
								logger.warn("Job FALSE : " + jobString);
							}
						}
						catch (Exception e) {
							logger.warn(e.getMessage(), e);
						}
						queue.jedisDel(CJobQueue.MDB_INDEX_RUNNING, jobString);
					}
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
