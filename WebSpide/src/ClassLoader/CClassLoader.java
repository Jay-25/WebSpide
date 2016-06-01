package ClassLoader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.logging.log4j.Logger;

import Log.CLog;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：SpideBase.Schedule.CSpideLoader
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:08:03
 * @version V1.0
 */
public class CClassLoader extends ClassLoader {

	private static Logger logger    = CLog.getLogger();
	private String        classPath = "." + File.separator;

	public CClassLoader() {

	}

	private CClassLoader(String path) {
		this.classPath = path;
	}

	public void setClassPath(String classPath) {
		this.classPath = classPath;
	}

	@SuppressWarnings("deprecation")
	@Override
	protected Class<?> findClass(String name) {
		String path = classPath + File.separator + name.replace(".", File.separator) + ".class";
		File file = new File(path);
		if (!file.isFile() || !file.exists()) {
			logger.error(path + " is not exist!");
			file = null;
			return null;
		}
		file = null;
		FileInputStream fi = null;
		ByteArrayOutputStream bt = null;
		try {
			fi = new FileInputStream(path);
			bt = new ByteArrayOutputStream();
			int p = 1;
			while ((p = fi.read()) != -1) {
				bt.write(p);
			}
			byte[] bytes = bt.toByteArray();
			return super.defineClass(bytes, 0, bytes.length);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		finally {
			try {
				if (fi != null) fi.close();
				fi = null;
				if (bt != null) bt.close();
				bt = null;
			}
			catch (IOException e) {
			}
		}
		return null;
	}

	/*
	 * path : path
	 * name : package.class
	 */
	public static Class<?> loadClass(final String path, final String name) {

		Class<?> c = null;
		CClassLoader classLoader = null;
		try {
			classLoader = new CClassLoader(path);
			c = classLoader.findLoadedClass(name);
			if (c == null) {
				c = classLoader.loadClass(name);
			}
			classLoader.resolveClass(c);
		}
		catch (Exception e) {
			logger.error(path + "." + name + " " + e);
		}
		finally {
			classLoader = null;
		}
		return c;
	}

	public static Class<?> loadClass(final String path, Class<?> clas) {
		return loadClass(path, clas.getName());
	}

	public static Class<?> loadClass(Class<?> clas) {
		return loadClass(clas.getResource("").getPath(), clas.getName());
	}

	public static Object loadInstance(final String path, final String name) {

		Class<?> c = loadClass(path, name);
		try {
			return c.newInstance();
		}
		catch (Exception e) {
			logger.error(path + "." + name + " " + e);
		}
		return null;
	}

	public static Object loadInstance(final String path, Class<?> clas) {
		return loadInstance(path, clas.getName());
	}

	public static Object loadInstance(Class<?> clas) {
		return loadInstance(clas.getResource("").getPath(), clas.getName());
	}

	/**
	 * 对象转数组
	 * 
	 * @param obj
	 * @return
	 */
	public static byte[] toByteArray(Object obj) {
		byte[] bytes = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		try {
			bos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(bos);
			oos.writeObject(obj);
			oos.flush();
			bytes = bos.toByteArray();
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		finally {
			try {
				if (bos != null) bos.close();
				bos = null;
				if (oos != null) oos.close();
				oos = null;
			}
			catch (IOException e) {
			}
		}
		return bytes;
	}

	/**
	 * 数组转对象
	 * 
	 * @param bytes
	 * @return
	 */
	public static Object toObject(byte[] bytes) {
		Object obj = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		try {
			bis = new ByteArrayInputStream(bytes);
			ois = new ObjectInputStream(bis);
			obj = ois.readObject();
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		finally {
			try {
				if (ois != null) ois.close();
				ois = null;
				if (bis != null) bis.close();
				bis = null;
			}
			catch (IOException e) {
			}
		}
		return obj;
	}
}
