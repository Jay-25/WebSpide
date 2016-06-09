package Job;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;

import Log.CLog;

public class CJobThread extends Thread {
	
	private final static Logger logger = CLog.getLogger();
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public CJobThread(Callable arg0, String arg1, int retry, long wait, long timeout) {
		super(new Runnable() {
			
			@Override
			public void run() {
				int retryTimes = retry;
				FutureTask<Object> task = new FutureTask<Object>(arg0);
				while (retryTimes-- > 0) {
					Thread thread = new Thread(task, arg1 + "-1");
					thread.start();
					try {
						task.get(timeout, TimeUnit.SECONDS);
						thread = null;
						break;
					}
					catch (InterruptedException | ExecutionException | TimeoutException e) {
						if (retryTimes == 0) {
							logger.error(e.getMessage(), e);
						}
						else {
							try {
								Thread.sleep(wait);
							}
							catch (InterruptedException e1) {
							}
						}
					}
					thread = null;
				}
				task = null;
			}
		}, arg1);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public CJobThread(Callable arg0, int retry, long wait, long timeout) {
		super(new Runnable() {
			
			@Override
			public void run() {
				int retryTimes = retry;
				FutureTask<Object> task = new FutureTask<Object>(arg0);
				while (retryTimes-- > 0) {
					Thread thread = new Thread(task);
					thread.start();
					try {
						task.get(timeout, TimeUnit.SECONDS);
						thread = null;
						break;
					}
					catch (InterruptedException | ExecutionException | TimeoutException e) {
						if (retryTimes == 0) {
							logger.error(e.getMessage(), e);
						}
						else {
							try {
								Thread.sleep(wait);
							}
							catch (InterruptedException e1) {
							}
						}
					}
					thread = null;
				}
				task = null;
			}
		});
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public CJobThread(Callable arg0, String arg1, long timeout) {
		super(new Runnable() {
			
			@Override
			public void run() {
				FutureTask<Object> task = new FutureTask<Object>(arg0);
				Thread thread = new Thread(task, arg1 + "-1");
				thread.start();
				try {
					task.get(timeout, TimeUnit.SECONDS);
				}
				catch (InterruptedException | ExecutionException | TimeoutException e) {
					logger.error(e.getMessage(), e);
				}
				task = null;
				thread = null;
			}
		}, arg1);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public CJobThread(Callable arg0, long timeout) {
		super(new Runnable() {
			
			@Override
			public void run() {
				FutureTask<Object> task = new FutureTask<Object>(arg0);
				Thread thread = new Thread(task);
				thread.start();
				try {
					task.get(timeout, TimeUnit.SECONDS);
				}
				catch (InterruptedException | ExecutionException | TimeoutException e) {
					logger.error(e.getMessage(), e);
				}
				task = null;
				thread = null;
			}
		});
	}
}
