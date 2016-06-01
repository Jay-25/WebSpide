/**
 * @Title: CJobDefaultWorker.java
 * @Package Job
 * @Description: TODO
 * @author
 * @date 2016-5-11 下午4:06:15
 * @version V1.0
 */
package Job;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Job.CJobDefaultWorker
 * @author：Zhao Jietong
 * @Create：2016-5-11 下午4:06:15
 * @version V1.0
 */

public abstract class CJobDefaultWorker implements IJobWorker {

	@Override
	public abstract boolean execute(String... args);
}
