package Extract.HtmlData;

import java.util.List;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.HtmlData.CTableDataExtract
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:23
 * @version V1.0
 */
public class CTableDataExtract {
	
	private Source        source       = null;
	private Element       elementTable = null;
	private List<Element> trList       = null;
	private int           maxRowNum    = 0;
	private int           maxColNum    = 0;
	
	public CTableDataExtract(String html) {
		source = new Source(html);
		elementTable = source.getAllElements(HTMLElementName.TABLE).get(0);
		trList = elementTable.getAllElements(HTMLElementName.TR);
		maxRowNum = trList.size();
		for (int i = 0; i < maxRowNum; i++) {
			int rn = trList.get(i).getAllElements(HTMLElementName.TD).size();
			if (rn > maxColNum) {
				maxColNum = rn;
			}
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		trList.clear();
		trList = null;
		source = null;
		super.finalize();
	}
	
	public String getData(int row, int col) {
		Element currentElement = getTableContent(elementTable, row, col);
		if (currentElement != null) {
			return currentElement.getContent().toString();
		}
		return null;
	}
	
	public int getMaxRowNum() {
		return maxRowNum;
	}
	
	public int getMaxColNum() {
		return maxColNum;
	}
	
	private Element getTableContent(Element element, int rows, int cols) {
		Element resultElement = null;
		if (rows < maxRowNum) {
			List<Element> tdList = trList.get(rows).getAllElements(HTMLElementName.TD);
			if (cols < tdList.size()) {
				resultElement = tdList.get(cols);
			}
		}
		return resultElement;
	}
	
	public String asText() {
		String result = "";
		for (int r = 0; r < maxRowNum; r++) {
			List<Element> tdList = trList.get(r).getAllElements(HTMLElementName.TD);
			result += (r + 1) + "	";
			for (int c = 0; c < tdList.size(); c++) {
				result += tdList.get(c).getContent().toString() + "	";
			}
			result += "\r\n";
		}
		return result;
	}
	
	public void print() {
		System.out.println(asText());
	}
}
