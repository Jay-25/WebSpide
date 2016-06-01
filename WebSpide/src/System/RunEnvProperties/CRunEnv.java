/**
 * @Title: CRunEnv.java
 * @Package System.RunEnvProperties
 * @Description: TODO
 * @author
 * @date 2014-10-17 下午3:53:45
 * @version V1.0
 */
package System.RunEnvProperties;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Properties;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：System.RunEnvProperties.CRunEnv
 * @author：Zhao Jietong
 * @Create：2014-10-17 下午3:53:45
 * @version V1.0
 */

public class CRunEnv {

	private static CRunEnv runEnv = null;

	private List<String>   JVM    = null;

	private Properties     props  = null;                // 系统属性

	private final String   java_version;                 // Java的运行环境版本
	private final String   java_vendor;                  // Java的运行环境供应商
	private final String   java_vendor_url;              // Java供应商的URL
	private final String   java_home;                    // Java的安装路径
	private final String   java_vm_specification_version; // Java的虚拟机规范版本
	private final String   java_vm_specification_vendor; // Java的虚拟机规范供应商
	private final String   java_vm_specification_name;   // Java的虚拟机规范名称
	private final String   java_vm_version;              // Java的虚拟机实现版本
	private final String   java_vm_vendor;               // Java的虚拟机实现供应商
	private final String   java_vm_name;                 // Java的虚拟机实现名称
	private final String   java_specification_version;   // Java运行时环境规范版本
	private final String   java_specification_vender;    // Java运行时环境规范供应商
	private final String   java_specification_name;      // Java运行时环境规范名称
	private final String   java_class_version;           // Java的类格式版本号
	private final String   java_class_path;              // Java的类路径
	private final String   java_library_path;            // 加载库时搜索的路径列表
	private final String   java_io_tmpdir;               // 默认的临时文件路径
	private final String   java_ext_dirs;                // 一个或多个扩展目录的路径
	private final String   os_name;                      // 操作系统的名称
	private final String   os_arch;                      // 操作系统的构架
	private final String   os_version;                   // 操作系统的版本
	private final String   file_separator;               // 文件分隔符
	private final String   path_separator;               // 路径分隔符
	private final String   line_separator;               // 行分隔符
	private final String   user_name;                    // 用户的账户名称
	private final String   user_home;                    // 用户的主目录
	private final String   user_dir;                     // 用户的当前工作目录

	private CRunEnv() {
		JVM = ManagementFactory.getRuntimeMXBean().getInputArguments();
		//
		props = System.getProperties(); // 系统属性
		java_version = props.getProperty("java.version"); // Java的运行环境版本
		java_vendor = props.getProperty("java.vendor"); // Java的运行环境供应商
		java_vendor_url = props.getProperty("java.vendor.url"); // Java供应商的URL
		java_home = props.getProperty("java.home"); // Java的安装路径
		java_vm_specification_version = props.getProperty("java.vm.specification.version"); // Java的虚拟机规范版本
		java_vm_specification_vendor = props.getProperty("java.vm.specification.vendor"); // Java的虚拟机规范供应商
		java_vm_specification_name = props.getProperty("java.vm.specification.name"); // Java的虚拟机规范名称
		java_vm_version = props.getProperty("java.vm.version"); // Java的虚拟机实现版本
		java_vm_vendor = props.getProperty("java.vm.vendor"); // Java的虚拟机实现供应商
		java_vm_name = props.getProperty("java.vm.name"); // Java的虚拟机实现名称
		java_specification_version = props.getProperty("java.specification.version"); // Java运行时环境规范版本
		java_specification_vender = props.getProperty("java.specification.vender"); // Java运行时环境规范供应商
		java_specification_name = props.getProperty("java.specification.name"); // Java运行时环境规范名称
		java_class_version = props.getProperty("java.class.version"); // Java的类格式版本号
		java_class_path = props.getProperty("java.class.path"); // Java的类路径
		java_library_path = props.getProperty("java.library.path"); // 加载库时搜索的路径列表
		java_io_tmpdir = props.getProperty("java.io.tmpdir"); // 默认的临时文件路径
		java_ext_dirs = props.getProperty("java.ext.dirs"); // 一个或多个扩展目录的路径
		os_name = props.getProperty("os.name"); // 操作系统的名称
		os_arch = props.getProperty("os.arch"); // 操作系统的构架
		os_version = props.getProperty("os.version"); // 操作系统的版本
		file_separator = props.getProperty("file.separator"); // 文件分隔符
		path_separator = props.getProperty("path.separator"); // 路径分隔符
		line_separator = props.getProperty("line.separator"); // 行分隔符
		user_name = props.getProperty("user.name"); // 用户的账户名称
		user_home = props.getProperty("user.home"); // 用户的主目录
		user_dir = props.getProperty("user.dir"); // 用户的当前工作目录
	}

	public static CRunEnv getInstance() {
		if (runEnv == null) runEnv = new CRunEnv();
		return runEnv;
	}

	public List<String> getJVM() {
		return JVM;
	}

	public String getJava_version() {
		return java_version;
	}

	public String getJava_vendor() {
		return java_vendor;
	}

	public String getJava_vendor_url() {
		return java_vendor_url;
	}

	public String getJava_home() {
		return java_home;
	}

	public String getJava_vm_specification_version() {
		return java_vm_specification_version;
	}

	public String getJava_vm_specification_vendor() {
		return java_vm_specification_vendor;
	}

	public String getJava_vm_specification_name() {
		return java_vm_specification_name;
	}

	public String getJava_vm_version() {
		return java_vm_version;
	}

	public String getJava_vm_vendor() {
		return java_vm_vendor;
	}

	public String getJava_vm_name() {
		return java_vm_name;
	}

	public String getJava_specification_version() {
		return java_specification_version;
	}

	public String getJava_specification_vender() {
		return java_specification_vender;
	}

	public String getJava_specification_name() {
		return java_specification_name;
	}

	public String getJava_class_version() {
		return java_class_version;
	}

	public String getJava_class_path() {
		return java_class_path;
	}

	public String getJava_library_path() {
		return java_library_path;
	}

	public String getJava_io_tmpdir() {
		return java_io_tmpdir;
	}

	public String getJava_ext_dirs() {
		return java_ext_dirs;
	}

	public String getOs_name() {
		return os_name;
	}

	public String getOs_arch() {
		return os_arch;
	}

	public String getOs_version() {
		return os_version;
	}

	public String getFile_separator() {
		return file_separator;
	}

	public String getPath_separator() {
		return path_separator;
	}

	public String getLine_separator() {
		return line_separator;
	}

	public String getUser_name() {
		return user_name;
	}

	public String getUser_home() {
		return user_home;
	}

	public String getUser_dir() {
		return user_dir;
	}

}
