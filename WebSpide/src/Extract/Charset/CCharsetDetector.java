package Extract.Charset;

import info.monitorenter.cpdetector.io.ASCIIDetector;
import info.monitorenter.cpdetector.io.ByteOrderMarkDetector;
import info.monitorenter.cpdetector.io.CodepageDetectorProxy;
import info.monitorenter.cpdetector.io.JChardetFacade;
import info.monitorenter.cpdetector.io.ParsingDetector;
import info.monitorenter.cpdetector.io.UnicodeDetector;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.Charset.CCharsetDetector
 * @author：Zhao Jietong
 * @Create：2014-9-18 下午2:17:30
 * @version V1.0
 */
public class CCharsetDetector {

	public static String detector(String text) {
		String charset = "";
		ByteArrayInputStream is = new ByteArrayInputStream(text.getBytes());
		charset = detector(is, text.length());
		try {
			is.close();
		}
		catch (IOException e) {
		}
		is = null;
		return charset;
	}

	public static String detector(ByteArrayInputStream is, int size) {
		String charset = "";
		ParsingDetector pd = new ParsingDetector(false);
		ByteOrderMarkDetector bomd = new ByteOrderMarkDetector();
		try {
			CodepageDetectorProxy proxy = CodepageDetectorProxy.getInstance();
			proxy.add(pd);
			proxy.add(bomd);
			proxy.add(JChardetFacade.getInstance());
			proxy.add(ASCIIDetector.getInstance());
			proxy.add(UnicodeDetector.getInstance());
			Charset cset = null;
			cset = proxy.detectCodepage(is, size);
			if (cset != null) {
				charset = cset.name();
			}
		}
		catch (Exception e) {
		}
		finally {
			try {
				is.close();
			}
			catch (IOException e) {
			}
			is = null;
			pd = null;
			bomd = null;
		}

		return charset;
	}
}
