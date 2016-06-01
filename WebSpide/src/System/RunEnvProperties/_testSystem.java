package System.RunEnvProperties;

public class _testSystem {

	public static void print() {
		CMonitorServiceImpl service = new CMonitorServiceImpl();
		CMonitorInfoBean monitorInfo;
		try {
			monitorInfo = service.getMonitorInfoBean();
			System.out.println("cpu占有率=" + monitorInfo.getCpuRatio());
			System.out.println("JVM中可使用内存=" + monitorInfo.getTotalMemory() + " kb");
			System.out.println("JVM中剩余内存=" + monitorInfo.getFreeMemory() + " kb");
			System.out.println("JVM中最大可使用内存=" + monitorInfo.getMaxMemory() + " kb");
			System.out.println("操作系统=" + monitorInfo.getOsName());
			System.out.println("总的物理内存=" + monitorInfo.getTotalMemorySize() + " kb");
			System.out.println("剩余的物理内存=" + monitorInfo.getFreeMemory() + " kb");
			System.out.println("已使用的物理内存=" + monitorInfo.getUsedMemory() + " kb");
			System.out.println("线程总数=" + monitorInfo.getTotalThread());
		}
		catch (Exception e) {
		}
		service = null;
	}
}
