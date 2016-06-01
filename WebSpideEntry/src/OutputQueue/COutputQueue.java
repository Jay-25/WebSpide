/**
 * @Title: COutputQueue.java
 * @Package OutputQueue
 * @Description: TODO
 * @author
 * @date 2016-5-23 下午3:52:14
 * @version V1.0
 */
package OutputQueue;

import Job.CJobQueue;
import Job.CJobQueueConfig;

/**
 * @Copyright：2016
 * @Project：WebSpideEntry
 * @Description：
 * @Class：OutputQueue.COutputQueue
 * @author：Zhao Jietong
 * @Create：2016-5-23 下午3:52:14
 * @version V1.0
 */
public class COutputQueue {
	
	public final static int  QUEUE_INDEX_OUTPUT  = 0;
	public final static int  MDB_INDEX_DUPLICATE = 1;
	private static CJobQueue outputQueue         = null;
	
	public static CJobQueue getOutputQueue(String configFile) {
		if (outputQueue == null) {
			CJobQueueConfig jobQueueConfig = new CJobQueueConfig(configFile);
			outputQueue = new CJobQueue(jobQueueConfig);
			outputQueue.setQueueName(jobQueueConfig.getQueueName() + "-OUTPUT");
		}
		return outputQueue;
	}
}
