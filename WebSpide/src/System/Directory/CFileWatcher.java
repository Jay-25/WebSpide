package System.Directory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

public class CFileWatcher implements Runnable {

	public static interface IFileWatcherDeal {

		public void deal(String fileName, String event);
	}

	private String           path            = null;
	private String[]         exNames         = null;
	private IFileWatcherDeal fileWatcherDeal = null;
	private Thread           thread          = null;
	private WatchService     watchService    = null;
	private boolean          stop            = false;

	public CFileWatcher(String path, String exname, IFileWatcherDeal fileWatcherDeal) {
		this.path = path;
		this.exNames = exname.split("\\|");
		this.fileWatcherDeal = fileWatcherDeal;
		thread = new Thread(this, "Trd-" + getClass().getName());
		thread.setDaemon(true);
	}

	public void start() {
		thread.start();
	}

	@Override
	public void run() {
		if (fileWatcherDeal == null) return;
		try {
			while (!stop) {
				watchService = FileSystems.getDefault().newWatchService();
				Paths.get(path).register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
				WatchKey key = watchService.take();
				for (WatchEvent<?> event : key.pollEvents()) {
					String fileName = event.context().toString();
					for (int i = 0; i < exNames.length; i++) {
						if (fileName.matches(".*\\" + exNames[i] + "$")) {
							// COutput.println(path + File.separator + fileName + "发生了" +
							// event.kind() + "事件");
							fileWatcherDeal.deal(path + File.separator + fileName, event.kind().name());
							break;
						}
					}
				}
			}
		}
		catch (Exception e) {
		}
	}

	@SuppressWarnings("deprecation")
	public void stop() {
		try {
			if (watchService != null) watchService.close();
		}
		catch (IOException e1) {
		}
		while (thread != null && thread.isAlive()) {
			stop = true;
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException e) {
			}
			try {
				thread.stop();
			}
			catch (Exception e1) {
			}
		}
		thread = null;
		watchService = null;
		fileWatcherDeal = null;
	}
}
