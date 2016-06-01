package Extract.HtmlData;

import Extract.Reduce.CHtmlReduce;
import Extract.Reduce.CHtmlTrim;
import Spider.CSpideExplorer;

import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class example_CHtml2Article {

	public static void main(String[] args) throws Exception {
		String url = "https://github.com/stanzhai/Html2Article";
		@SuppressWarnings("resource")
		CSpideExplorer explorer = new CSpideExplorer();
		HtmlPage htmlPage = explorer.getPage(url);
		htmlPage = CHtmlTrim.removeHidenElement(htmlPage);
		CHtml2Article html2Article = new CHtml2Article();
		String title = html2Article.GetTitle(htmlPage.asXml());
		String html = new CHtmlReduce().reduce(htmlPage.asXml());
		String mtext = html2Article.GetContent(html, true);
		mtext = new CHtmlReduce(false).reduce("<title>" + title + "</title>" + mtext);
		mtext = CHtmlTrim.translateHTMLEscape(mtext);
		System.out.println(mtext);
	}
}
