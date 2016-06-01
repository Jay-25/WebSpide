package Algorithm.Math;

import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class CEncry {
	
	public static String md5(String plainText) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte b[] = md.digest();
			int i;
			StringBuffer buf = new StringBuffer("");
			for (int offset = 0; offset < b.length; offset++) {
				i = b[offset];
				if (i < 0) i += 256;
				if (i < 16) buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			return buf.toString();
		}
		catch (NoSuchAlgorithmException e) {
		}
		return null;
	}
	
	/***
	 * encode by Base64
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static String encodeBase64(byte[] input) throws Exception {
		Class clazz = Class.forName("com.sun.org.apache.xerces.internal.impl.dv.util.Base64");
		Method mainMethod = clazz.getMethod("encode", byte[].class);
		mainMethod.setAccessible(true);
		Object retObj = mainMethod.invoke(null, new Object[] { input });
		return (String) retObj;
	}
	
	/***
	 * decode by Base64
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static byte[] decodeBase64(String input) throws Exception {
		Class clazz = Class.forName("com.sun.org.apache.xerces.internal.impl.dv.util.Base64");
		Method mainMethod = clazz.getMethod("decode", String.class);
		mainMethod.setAccessible(true);
		Object retObj = mainMethod.invoke(null, input);
		return (byte[]) retObj;
	}
	
	//
	public static byte[] AES_CBC_Encrypt(byte[] data, String key, String iv) {
		try {
			if (key == null) {
				System.out.print("Key为空null");
				return null;
			}
			// 判断Key是否为16位  
			if (key.length() != 16) {
				System.out.print("Key长度不是16位");
				return null;
			}
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");//"算法/模式/补码方式"  
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key.getBytes(), "AES"), new IvParameterSpec(iv.getBytes())); //使用CBC模式，需要一个向量iv，可增加加密算法的强度  
			int len = data.length;
			/* 计算补0后的长度 */
			while (len % 16 != 0)
				len++;
			byte[] sraw = new byte[len];
			for (int i = 0; i < len; ++i) { /* 在最后补0 */
				sraw[i] = (i < data.length) ? data[i] : 0;
			}
			return cipher.doFinal(sraw);//此处使用BASE64做转码功能，同时能起到2次加密的作用。  
		}
		catch (Exception ex) {
			System.out.println(ex.toString());
			return null;
		}
	}
	
	// 解密  
	public static byte[] AES_CBC_Decrypt(byte[] data, String key, String iv) {
		try {
			// 判断Key是否正确  
			if (key == null) {
				System.out.print("Key为空null");
				return null;
			}
			// 判断Key是否为16位  
			if (key.length() != 16) {
				System.out.print("Key长度不是16位");
				return null;
			}
			Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key.getBytes(), "AES"), new IvParameterSpec(iv.getBytes()));
			return cipher.doFinal(data);
		}
		catch (Exception ex) {
			System.out.println(ex.toString());
			return null;
		}
	}
}
