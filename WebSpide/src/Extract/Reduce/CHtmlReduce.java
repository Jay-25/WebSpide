package Extract.Reduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.w3c.tidy.Tidy;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.Reduce.CHtmlReduce
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:30
 * @version V1.0
 */
public class CHtmlReduce {
	
	private final Tidy tidy     = new Tidy();
	boolean            bodyOnly = true;
	
	public CHtmlReduce() {
		config();
	}
	
	public CHtmlReduce(boolean bodyOnly) {
		this.bodyOnly = bodyOnly;
		config();
	}
	
	private void config() {
		tidy.setXmlOut(true); // 输出xml
		tidy.setXmlPi(true); // 文件头输出xml标记
		tidy.setXHTML(false);// 输出xhtml
		tidy.setPrintBodyOnly(bodyOnly); // 只输出body中的部分
		tidy.setForceOutput(true); // 不管生成的xml是否有错，强制输出
		tidy.setShowErrors(0); // 不显示错误信息
		tidy.setShowWarnings(false); // 不显示警告信息
		tidy.setDropProprietaryAttributes(true);
		tidy.setDropFontTags(true); // 删除字体节点
		tidy.setDropEmptyParas(true); // 删除空段落
		tidy.setLogicalEmphasis(true); // 用em替代i，strong替代b
		tidy.setHideComments(true);// 结果中不生成注释
		tidy.setTrimEmptyElements(true); // 不输出空元素
		tidy.setFixComments(true); // 修复注释
		tidy.setFixBackslash(true); // 修复反斜杆
		tidy.setMakeClean(true); // 删除混乱的表示
		tidy.setQuiet(true);// 不显示警告信息
		tidy.setQuoteNbsp(false); // 将空格输出为 &nbsp;
		tidy.setQuoteMarks(false); // 将双引号输出为 &quot;
		tidy.setQuoteAmpersand(true); // 将 &amp; 输出为 &
		tidy.setSmartIndent(true); // 节点结束后，是否另起一行
		tidy.setIndentContent(false);// 是否使用缩进
		tidy.setIndentAttributes(false); // 属性是否另起一行
		tidy.setWraplen(0); // 多长换行，0是不自动换行
		tidy.setWord2000(true); // 清洗word2000的内容
	}
	
	public String reduce(String text) {
		return reduce(text, "utf-8"); // iso-8859-1
	}
	
	public String reduce(String text, String charset) {
		text = text.replaceAll("(?is)<script.*?>.*?</script>", ""); // remove javascript
		ByteArrayInputStream is = new ByteArrayInputStream(text.getBytes());
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		// charset = CCharsetDetector.detector(text);
		tidy.setInputEncoding(charset); // 输入编码
		tidy.setOutputEncoding(charset); // 输出编码
		tidy.parse(is, os);
		String result = os.toString();
		is = null;
		os = null;
		return result;
	}
}
