package Extract.Reduce;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gargoylesoftware.htmlunit.html.DomElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.Reduce.CHtmlTrim
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:35
 * @version V1.0
 */
public class CHtmlTrim {
	
	/*
	 * 去除元素中的属性信息，resAtts中的属性除外
	 */
	public static String dropAttributes(String text, String[] resAtts) {
		String result = "";
		String lines[] = text.split("\n");
		for (int n = 0; n < lines.length; n++) {
			lines[n] = lines[n].trim();
		}
		for (int n = 0; n < lines.length; n++) {
			int rn = 1;
			String rs = "$1 ";
			String regex = "([/a-zA-Z]+)[^<>]*?";
			if (resAtts != null) {
				for (int i = 0; i < resAtts.length; i++) {
					if (lines[n].indexOf(resAtts[i] + "=") > 0) {
						regex += "(" + resAtts[i] + "=[\"|'][^\"'<>]*[\"|'])[^<>]*?";
						rs += "$" + (++rn) + " ";
					}
				}
			}
			result += lines[n].replaceAll("<" + regex + ">", "<" + rs.trim() + ">");
		}
		lines = null;
		return result;
	}
	
	/*
	 * 递归替换
	 */
	public static String truncateReplaceAll(String str, String regex, String str2) {
		Matcher m;
		do {
			System.out.println("--------");
			str = str.replaceAll(regex, str2);
			System.out.println("====");
			m = Pattern.compile(regex, Pattern.MULTILINE).matcher(str);
		} while (m.find());
		return str;
	}
	
	/*
	 * 去除单个元素，元素名由elms指定
	 */
	public static String dropSingleElement(String text, String[] elms) {
		for (int i = 0; i < elms.length; i++) {
			text = truncateReplaceAll(text, "(.*?)<" + elms[i] + ".*?>[ \n]*</" + elms[i] + ">(.*?)", "$1$2");
		}
		// text = CHtmlTrim.truncateReplaceAll(text, ">[  　]+([^<> 　 ]+)", ">$1");
		// text = CHtmlTrim.truncateReplaceAll(text, ">([^<> 　 ]+)[  　]+", ">$1");
		return text;
	}
	
	/*
	 * 去除重复空格
	 */
	public static String truncateBlank(String str) {
		return str.replaceAll("\\s{1,}", " ");
	}
	
	/*
	 * html字符标准化
	 */
	public static String trim(String str) {
		return str.replaceAll("\r\n", "\\n").replaceAll("：", ":").replaceAll("([，、])", ",").replaceAll("；", ";").replaceAll("－", "-").replaceAll("［", "[").replaceAll("］", "]")
		                .replaceAll("（", "(").replaceAll("）", ")").replaceAll("｛", "{").replaceAll("｝", "}").replaceAll("	", " ")
		                .replaceAll("[^a-zA-Z0-9 ,:;%#@~=_/\\|\\.\\+\\-\\*\\(\\)\\[\\]\"\'{}<>㎡m³\u4e00-\u9fa5]", " ").replaceAll("([\\+\\*\\^\\?`!&$])", "")
		                .replaceAll("(?s)([;]+)(?=.*)", ";").replaceAll("(?s)([,]+)(?=.*)", ",").replaceAll("(?s)([\"]+)(?=.*)", "\"").replaceAll("(?s)([\']+)(?=.*)", "'")
		                .replaceAll("(?s)([ 	　,;\\|]+)(?=.*)", " ").replaceAll("(?s)([\\]]+)(?=.*)", "]").replaceAll("(?s)([\\[]+)(?=.*)", "[").replaceAll("(?s)([\\)]+)(?=.*)", ")")
		                .replaceAll("(?s)([\\(]+)(?=.*)", "(").replaceAll("(\\s){1,}", "$1");
	}
	
	/*
	 * 全角转半角代
	 */
	public static String replaceDBC2SBC(String input) {
		Matcher m = Pattern.compile("[\u3000\uff01-\uff5f]{1}").matcher(input);
		StringBuffer s = new StringBuffer();
		while (m.find()) {
			char c = m.group(0).charAt(0);
			char replacedChar = c == '　' ? ' ' : (char) (c - 0xfee0);
			m.appendReplacement(s, String.valueOf(replacedChar));
		}
		m.appendTail(s);
		return s.toString();
	}
	
	public static String removeScript(String html) {
		return html.replaceAll("(?is)<script.*?>.*?</script>", "");
	}
	
	public static HtmlPage removeScriptElement(HtmlPage htmlPage) {
		List<DomElement> elements = htmlPage.getElementsByTagName("script");
		for (int i = elements.size() - 1; i >= 0; i--) {
			elements.get(i).remove();
		}
		return htmlPage;
	}
	
	public static HtmlPage removeHidenElement(HtmlPage htmlPage) {
		@SuppressWarnings("unchecked")
		List<DomElement> elements = (List<DomElement>) htmlPage.getByXPath("//*[@style]");
		for (int i = elements.size() - 1; i >= 0; i--) {
			String str = elements.get(i).asXml();
			if (Pattern.compile("^<.+?display[ ]*:[ ]*none.+?>", Pattern.MULTILINE).matcher(str).find()) {
				elements.get(i).remove();
			}
			else if (Pattern.compile("^<.+?visibility[ ]*:[ ]*hidden.+?>", Pattern.MULTILINE).matcher(str).find()) {
				elements.get(i).remove();
			}
		}
		return htmlPage;
	}
	
	public static String translateHTMLEscape(String html) {
		html = html.replaceAll("&lt;", "<");
		html = html.replaceAll("&gt;", ">");
		html = html.replaceAll("&amp;", "&");
		html = html.replaceAll("&quot;", "\"");
		html = html.replaceAll("&nbsp;", " ");
		html = html.replaceAll("&copy;", "©");
		html = html.replaceAll("&reg;", "®");
		html = html.replaceAll("&cent;", "￠");
		html = html.replaceAll("&pound;", "￡");
		html = html.replaceAll("&curren;", "¤");
		html = html.replaceAll("&yen;", "￥");
		html = html.replaceAll("&brvbar;", "|");
		html = html.replaceAll("&sect;", "§");
		html = html.replaceAll("&uml;", "¨");
		html = html.replaceAll("&ordf;", "a");
		html = html.replaceAll("&macr;", "ˉ");
		html = html.replaceAll("&deg;", "°");
		html = html.replaceAll("&plusmn;", "±");
		html = html.replaceAll("&sup1;", "1");
		html = html.replaceAll("&sup2;", "2");
		html = html.replaceAll("&sup3;", "3");
		html = html.replaceAll("&acute;", "′");
		html = html.replaceAll("&micro;", "μ");
		html = html.replaceAll("&middot;", "·");
		html = html.replaceAll("&ordm;", "o");
		html = html.replaceAll("&Agrave;", "À");
		html = html.replaceAll("&Aacute;", "Á");
		html = html.replaceAll("&circ;", "Â");
		html = html.replaceAll("&Atilde;", "Ã");
		html = html.replaceAll("&Auml;", "Ä");
		html = html.replaceAll("&ring;", "Å");
		html = html.replaceAll("&AElig;", "Æ");
		html = html.replaceAll("&Ccedil;", "Ç");
		html = html.replaceAll("&Egrave;", "È");
		html = html.replaceAll("&Eacute;", "É");
		html = html.replaceAll("&Ecirc;", "Ê");
		html = html.replaceAll("&Euml;", "Ë");
		html = html.replaceAll("&Igrave;", "Ì");
		html = html.replaceAll("&Iacute;", "Í");
		html = html.replaceAll("&Icirc;", "Î");
		html = html.replaceAll("&Iuml;", "Ï");
		html = html.replaceAll("&ETH;", "Ð");
		html = html.replaceAll("&Ntilde;", "Ñ");
		html = html.replaceAll("&Ograve;", "Ò");
		html = html.replaceAll("&Oacute;", "Ó");
		html = html.replaceAll("&Ocirc;", "Ô");
		html = html.replaceAll("&Otilde;", "Õ");
		html = html.replaceAll("&Ouml;", "Ö");
		html = html.replaceAll("&Oslash;", "Ø");
		html = html.replaceAll("&Ugrave;", "Ù");
		html = html.replaceAll("&Uacute;", "Ú");
		html = html.replaceAll("&Ucirc;", "Û");
		html = html.replaceAll("&Uuml;", "Ü");
		html = html.replaceAll("&Yacute;", "Ý");
		html = html.replaceAll("&THORN;", "Þ");
		html = html.replaceAll("&szlig;", "ß");
		html = html.replaceAll("&agrave;", "à");
		html = html.replaceAll("&aacute;", "á");
		html = html.replaceAll("&acirc;", "â");
		html = html.replaceAll("&atilde;", "ã");
		html = html.replaceAll("&auml;", "ä");
		html = html.replaceAll("&aring;", "å");
		html = html.replaceAll("&aelig;", "æ");
		html = html.replaceAll("&ccedil;", "ç");
		html = html.replaceAll("&egrave;", "è");
		html = html.replaceAll("&eacute;", "é");
		html = html.replaceAll("&ecirc;", "ê");
		html = html.replaceAll("&euml;", "ë");
		html = html.replaceAll("&igrave;", "ì");
		html = html.replaceAll("&iacute;", "í");
		html = html.replaceAll("&icirc;", "î");
		html = html.replaceAll("&iuml;", "ï");
		html = html.replaceAll("&ieth;", "ð");
		html = html.replaceAll("&ntilde;", "ñ");
		html = html.replaceAll("&ograve;", "ò");
		html = html.replaceAll("&oacute;", "ó");
		html = html.replaceAll("&ocirc;", "ô");
		html = html.replaceAll("&otilde;", "õ");
		html = html.replaceAll("&ouml;", "ö");
		html = html.replaceAll("&divide;", "÷");
		html = html.replaceAll("&oslash;", "ø");
		html = html.replaceAll("&ugrave;", "ù");
		html = html.replaceAll("&uacute;", "ú");
		html = html.replaceAll("&ucirc;", "û");
		html = html.replaceAll("&uuml;", "ü");
		html = html.replaceAll("&yacute;", "ý");
		html = html.replaceAll("&thorn;", "þ");
		html = html.replaceAll("&yuml;", "ÿ");
		return html;
	}
	
	public static String removeHtmlTag(String html) {
		html = html.replaceAll("\n", "");
		html = html.replaceAll("(?is)<!DOCTYPE.*?>", "");
		html = html.replaceAll("(?is)<!--.*?-->", ""); // remove html comment
		html = html.replaceAll("(?is)<style.*?>.*?</style>", ""); // remove css
		html = html.replaceAll("(?is)<script.*?>.*?</script>", ""); // remove javascript
		html = html.replaceAll("(?is)<noscript.*?>.*?</noscript>", ""); // remove javascript
		html = html.replaceAll("(?is)<[ ]*(/h\\d|/table|/th|/tr|/p|/li|/ul)?[ ]*>", "\n");
		html = html.replaceAll("(?is)<.+?>", "");
		html = translateHTMLEscape(html);
		//
		html = html.replaceAll("&.{2,5};|&#.{2,5};", " "); // remove special char
		html = html.replaceAll("[	  ]", " ");
		html = html.replaceAll("\\s*\n\\s*", "\n");
		html = html.replaceAll("(\\s){1,}", "$1");
		html = html.replaceAll("[ ]+?(\\s+)", "$1").replaceAll("(\\s)[ ]+", "$1");
		html = html.replaceAll("[ ]+?(\\p{P})", "$1").replaceAll("(\\p{P})[ ]+", "$1");
		return html;
	}
}
