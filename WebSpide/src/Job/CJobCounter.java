package Job;

import java.util.concurrent.atomic.AtomicLong;

public class CJobCounter {
	
	private final AtomicLong maxNum       = new AtomicLong(1);
	private final AtomicLong jobNum       = new AtomicLong(0);
	private final AtomicLong lastQueueLen = new AtomicLong(0);
	
	public void init(long N) {
		init(N, N);
	}
	
	public void init(long jobN, long maxN) {
		lastQueueLen.set(maxN);
		if (maxN <= 0) {
			jobNum.set(1);
			maxNum.set(1);
		}
		else {
			if (jobN < maxN) {
				jobNum.set(jobN);
				maxNum.set(jobN);
			}
			else {
				jobNum.set(maxN);
				maxNum.set(maxN);
			}
		}
	}
	
	public void update(long maxN) {
		if (lastQueueLen.get() != maxN) {
			lastQueueLen.set(maxN);
			if (lastQueueLen.get() < maxNum.get()) {
				maxNum.set(lastQueueLen.get());
			}
		}
	}
	
	public boolean jobIsRunable() {
		return jobNum.get() > 0;
	}
	
	public boolean hasBusyJob() {
		return jobNum.get() < maxNum.get();
	}
	
	public void decrement() {
		if (jobNum.get() > 0) jobNum.decrementAndGet();
	}
	
	public void increment() {
		if (jobNum.get() < maxNum.get()) {
			jobNum.incrementAndGet();
		}
	}
	
	public void resetJobNum() {
		jobNum.set(0);
	}
	
	public long getJobNum() {
		return jobNum.get();
	}
	
	public long getMaxNum() {
		return maxNum.get();
	}
}
