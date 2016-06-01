/**
 * @author Zhao Jietong 2014-5-2 下午6:27:15
 */
package Geography;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import Spider.CAdvanceSpideExplorer;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.xml.XmlPage;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Geography.CGeography
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:07:02
 * @version V1.0
 */
public class CGeography {

	static public enum FROM {
		BAIDU, GOOGLE, GAODE,
	}

	private static CGeography geography = null;

	private CGeography() {
	}

	public static CGeography getInstance() {
		if (geography == null) {
			geography = new CGeography();
		}
		return geography;
	}

	public CDataCoordinate getCoordinate_Baidu(String address) {
		if (address.length() < 2) return null;
		CDataCoordinate coordinate = null;
		for (int i = 0; i < 3; i++) {
			try {
				CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
				XmlPage xmlPage = explorer.getExplorer().getPage(String.format("http://api.map.baidu.com/geocoder?address=%s&output=xml&key=aVA53MLrilAbsnFCmugLQYVA", java.net.URLEncoder.encode(address, "UTF-8")));
				explorer.close();
				explorer = null;
				if (xmlPage != null) {
					coordinate = new CDataCoordinate();
					DealHtml_Baidu(xmlPage, coordinate);
				}
				break;
			}
			catch (Exception e) {
			}
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e1) {
			}
		}
		return coordinate;
	}

	public CDataCoordinate getCoordinate_Google(String address) {
		if (address.length() < 2) return null;
		CDataCoordinate coordinate = null;
		for (int i = 0; i < 3; i++) {
			try {
				CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
				XmlPage xmlPage = explorer.getExplorer().getPage(String.format("http://ditu.google.cn/maps/api/geocode/xml?address=%s&sensor=false&language=zh-CN", java.net.URLEncoder.encode(address, "UTF-8")));
				explorer.close();
				explorer = null;
				coordinate = new CDataCoordinate();
				DealHtml_Google(xmlPage, coordinate);
				break;
			}
			catch (Exception e) {
			}
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e1) {
			}
		}
		return coordinate;
	}

	public CDataCoordinate getCoordinate_GaoDe(String address) {
		if (address.length() < 2) return null;
		CDataCoordinate coordinate = null;
		for (int i = 0; i < 3; i++) {
			try {
				CAdvanceSpideExplorer explorer = new CAdvanceSpideExplorer(BrowserVersion.CHROME);
				XmlPage xmlPage = explorer.getExplorer().getPage(
				                String.format("http://api.amap.com:9090/geocode/simple?resType=xml&encode=utf-8&range=300&roadnum=3&crossnum=2&poinum=2&retvalue=1&key=undefined&sid=7000&address=%s&rid=89616", java.net.URLEncoder.encode(address, "UTF-8")));
				explorer.close();
				explorer = null;
				coordinate = new CDataCoordinate();
				DealHtml_GaoDe(xmlPage, coordinate);
				break;
			}
			catch (Exception e) {
			}
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e1) {
			}
		}
		return coordinate;
	}

	//
	protected void DealHtml_Baidu(XmlPage xmlPage, Object object) {
		String tmp = null;
		CDataCoordinate coordinate = (CDataCoordinate) object;
		coordinate.setStyle("BaiDu");
		if (xmlPage.getXmlDocument().getElementsByTagName("status").item(0).getTextContent().equals("OK")) {
			tmp = xmlPage.getXmlDocument().getElementsByTagName("lat").item(0).getTextContent().trim();
			if (tmp.length() > 0) {
				coordinate.setLatitude(Double.valueOf(tmp));
			}
			tmp = xmlPage.getXmlDocument().getElementsByTagName("lng").item(0).getTextContent().trim();
			if (tmp.length() > 0) {
				coordinate.setLongitude(Double.valueOf(tmp));
			}
		}
	}

	protected void DealHtml_Google(XmlPage xmlPage, Object object) {
		String regEx = null;
		Pattern pat = null;
		Matcher mat = null;
		boolean rs = false;
		NodeList elm = null;
		String _address = null;
		CDataCoordinate coordinate = (CDataCoordinate) object;
		coordinate.setStyle("Google");
		if (xmlPage.getXmlDocument().getElementsByTagName("status").item(0).getTextContent().equals("OK")) {
			regEx = "(.*)邮政编码:(.*)";
			pat = Pattern.compile(regEx);
			elm = xmlPage.getXmlDocument().getElementsByTagName("result");
			int sum = elm.getLength();
			while (sum > 0) {
				sum--;
				_address = ((Element) elm.item(sum)).getElementsByTagName("formatted_address").item(0).getTextContent();
				mat = pat.matcher(_address);
				rs = mat.find();
				if (rs) {
					coordinate.setAddress(mat.group(1).trim());
					coordinate.setPostcode(mat.group(2).trim());
				}
				else {
					regEx = "(.*)邮编:(.*)";
					pat = Pattern.compile(regEx);
					mat = pat.matcher(_address);
					rs = mat.find();
					if (rs) {
						coordinate.setAddress(mat.group(1).trim());
						coordinate.setPostcode(mat.group(2).trim());
					}
					else {
						coordinate.setAddress(_address);
					}
				}
				coordinate.setLatitude(Double.valueOf(((Element) ((Element) elm.item(sum)).getElementsByTagName("location").item(0)).getElementsByTagName("lat").item(0).getTextContent().trim()));
				coordinate.setLongitude(Double.valueOf(((Element) ((Element) elm.item(sum)).getElementsByTagName("location").item(0)).getElementsByTagName("lng").item(0).getTextContent().trim()));
			}
		}
		regEx = null;
		pat = null;
		mat = null;
		elm = null;
		_address = null;
	}

	protected void DealHtml_GaoDe(XmlPage xmlPage, Object object) {
		String regEx = null;
		Pattern pat = null;
		Matcher mat = null;
		boolean rs = false;
		NodeList elm = null;
		String _address = null;
		CDataCoordinate coordinate = (CDataCoordinate) object;
		coordinate.setStyle("GaoDe");
		if (xmlPage.getXmlDocument().getElementsByTagName("status").item(0).getTextContent().equals("E0")) {
			regEx = "(.*)邮政编码:(.*)";
			pat = Pattern.compile(regEx);
			elm = xmlPage.getXmlDocument().getElementsByTagName("poi");
			int sum = elm.getLength();
			while (sum > 0) {
				sum--;
				_address = ((Element) elm.item(sum)).getElementsByTagName("name").item(0).getTextContent();
				mat = pat.matcher(_address);
				rs = mat.find();
				if (rs) {
					coordinate.setAddress(mat.group(1).trim());
					coordinate.setPostcode(mat.group(2).trim());
				}
				else {
					regEx = "(.*)邮编:(.*)";
					pat = Pattern.compile(regEx);
					mat = pat.matcher(_address);
					rs = mat.find();
					if (rs) {
						coordinate.setAddress(mat.group(1).trim());
						coordinate.setPostcode(mat.group(2).trim());
					}
					else {
						coordinate.setAddress(_address);
					}
				}
				coordinate.setLatitude(Double.valueOf(((Element) elm.item(sum)).getElementsByTagName("y").item(0).getTextContent().trim()));
				coordinate.setLongitude(Double.valueOf(((Element) elm.item(sum)).getElementsByTagName("x").item(0).getTextContent().trim()));
			}
		}
		regEx = null;
		pat = null;
		mat = null;
		elm = null;
		_address = null;
	}
}
