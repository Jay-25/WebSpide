/**
 * @Title: CDoc2Html.java
 * @Package Convert.FileConvert
 * @Description: TODO
 * @author
 * @date 2015-5-4 下午5:58:35
 * @version V1.0
 */
package Convert.FileConvert;

import java.io.File;
import java.io.FileInputStream;

import org.apache.poi.POIXMLDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;

import System.Directory.CFile;

// xwpf专门加强处理Word2007 .docx 格式
/**     
 * @Copyright：2015
 * @Project：Spide     
 *  
 * @Description：  
 * @Class：Convert.FileConvert.CDoc2Html       
 * @author：Zhao Jietong
 * @Create：2015-5-4 下午5:58:35     
 * @version   V1.0      
 */
public class CDoc2Text {
	
	//	public static void main(String[] args) {
	//
	//		System.out.println("获取整个word文本内容:");
	//		System.out.println(new CDoc2Text().getText("D:\\2014年app.docx")); //"D:\\2014年app.docx"   "D:\\11111.doc"
	//
	//	}
	public String getText(String filePath) {
		String text = null;
		try {
			File file = new File(filePath);
			String ex = CFile.getFileExtension(file).toLowerCase();
			if (ex.equals("doc")) {
				FileInputStream fis = new FileInputStream(file);
				WordExtractor wordExtractor = new WordExtractor(fis);
				text = wordExtractor.getText();
				wordExtractor.close();
				fis.close();
			}
			else if (ex.equals("docx")) {
				// 需要使用xmlbeans包，但是使用了该包就不能使用ClassLoader包外的class文件，这是xmlbeans的bug
				XWPFWordExtractor docx = new XWPFWordExtractor(POIXMLDocument.openPackage(filePath));
				text = docx.getText();
				docx.close();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return text;
	}
}
