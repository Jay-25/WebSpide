/**
 * @Title: CSpideVersion.java
 * @Package SpideBase
 * @Description: TODO
 * @author
 * @date 2014-10-13 下午5:35:36
 * @version V1.0
 */
package Job;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：SpideBase.CSpideVersion
 * @author：Zhao Jietong
 * @Create：2014-10-13 下午5:35:36
 * @version V1.0
 */
public class CSpideVersion {

	public static final String VERSION     = "1.0";               // 版本
	public static final String COMPILEDATE = "2016-5-18 09:44:07"; // 编译日期
	public static boolean      TEST        = !true;               // 测试

	public CSpideVersion(boolean test) {
		TEST = test;
	}

	public static void printVersion(String name) {
		System.out.println("SpideFrame: " + name);
		System.out.println("Version   : " + VERSION + (TEST ? " (Test)" : ""));
		System.out.println("BuildTime : " + COMPILEDATE);
		System.out.println("\n----- INIT -----\n");
	}
}
