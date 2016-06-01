package PageParser;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：SpideEntry.CSpideEntity_HtmlUnit
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:08:12
 * @version V1.0
 */
public class CPageParse {

	private static Pattern pattern1 = Pattern.compile("^(https?|ftp|file)://");
	private static Pattern pattern2 = Pattern.compile("^javascript:");

	@SuppressWarnings("unchecked")
	protected List<HtmlAnchor> allLinks(HtmlPage htmlPage) {
		return (List<HtmlAnchor>) htmlPage.getByXPath("//*[@href]");
	}

	protected <T> HashSet<T> links(HtmlPage htmlPage, Object... paras) {
		return null;
	}

	protected HashSet<String> linksStrings(HtmlPage htmlPage, String... regexs) {
		HashSet<String> urls = new HashSet<String>();
		List<HtmlAnchor> hrefs = allLinks(htmlPage);
		if (hrefs == null) {
			return null;
		}
		if (regexs.length == 0) {
			regexs = new String[1];
			regexs[0] = ".*";
		}
		int size = hrefs.size();
		for (int i = 0; i < size; i++) {
			try {
				String url = hrefs.get(i).getHrefAttribute();
				for (String regex : regexs) {
					if (pattern1.matcher(regex).find() && !pattern1.matcher(url).find() && !pattern2.matcher(url).find()) {
						url = htmlPage.getUrl().getProtocol() + "://" + htmlPage.getUrl().getHost() + url;
					}
					if (url.matches(regex)) {
						urls.add(url);
						break;
					}
				}
			}
			catch (Exception e) {
			}
		}
		return urls;
	}

	protected HashSet<HtmlAnchor> linksElements(HtmlPage htmlPage, String... regexs) {
		HashSet<HtmlAnchor> urls = new HashSet<HtmlAnchor>();
		List<HtmlAnchor> hrefs = allLinks(htmlPage);
		if (hrefs == null) {
			return null;
		}
		if (regexs.length == 0) {
			regexs = new String[1];
			regexs[0] = ".*";
		}
		int size = hrefs.size();
		for (int i = 0; i < size; i++) {
			try {
				String url = hrefs.get(i).getHrefAttribute();
				for (String regex : regexs) {
					if (pattern1.matcher(regex).find() && !pattern1.matcher(url).find() && !pattern2.matcher(url).find()) {
						url = htmlPage.getUrl().getProtocol() + "://" + htmlPage.getUrl().getHost() + url;
					}
					if (url.matches(regex)) {
						urls.add(hrefs.get(i));
						break;
					}
				}
			}
			catch (Exception e) {
			}
		}
		return urls;
	}
}
