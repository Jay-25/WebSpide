/**
 * @Title: CString2Xml.java
 * @Package Extract.Reduce
 * @Description: TODO
 * @author
 * @date 2014-9-30 下午2:23:21
 * @version V1.0
 */
package Extract.Reduce;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.Reduce.CString2Xml
 * @author：Zhao Jietong
 * @Create：2014-9-30 下午2:23:21
 * @version V1.0
 */
public class CXmlConverter {
	
	public static Document string2Xml(String text) {
		Document doc = null;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(new StringReader(text)));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return doc;
	}
	
	public static String xml2String(Document doc) {
		try {
			Transformer t = TransformerFactory.newInstance().newTransformer();
			// t.setOutputProperty("encoding", "GB23121");// 解决中文问题,试过用GBK不行
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			t.transform(new DOMSource(doc), new StreamResult(bos));
			return bos.toString();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
}
