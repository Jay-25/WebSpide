package Extract.HtmlData;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Extract.Reduce.CHtmlTrim;

/**
 * 网页正文提取 
 * 
 * @author https://github.com/zhaojietong/Html2Article
 */
public class CHtml2Article {
	
	private boolean appendMode        = true; // 是否使用追加模式，默认为true,设置为true会将更多符合条件的文本添加到正文
	private int     blocksWidth       = 6;   // 按行分析的深度，默认为6
	private int     limitCount        = 180; // 字符限定数，当分析的文本数量达到限定数则认为进入正文内容
	private int     headEmptyLines    = 2;   // 确定文章正文头部时，向上查找，连续的空行到达_headEmptyLines，则停止查找
	private int     endLimitCharCount = 2;   // 用于确定文章结束的字符数
	
	// 是否使用追加模式，默认为true,设置为true会将更多符合条件的文本添加到正文
	public void setAppendMode(boolean appendMode) {
		this.appendMode = appendMode;
	}
	
	// 按行分析的深度，默认为6
	public void setBlocksWidth(int blocksWidth) {
		this.blocksWidth = blocksWidth;
	}
	
	// 字符限定数，当分析的文本数量达到限定数则认为进入正文内容
	public void setLimitCount(int limitCount) {
		this.limitCount = limitCount;
	}
	
	// 确定文章正文头部时，向上查找，连续的空行到达_headEmptyLines，则停止查找
	public void setHeadEmptyLines(int headEmptyLines) {
		this.headEmptyLines = headEmptyLines;
	}
	
	// 用于确定文章结束的字符数
	public void setEndLimitCharCount(int endLimitCharCount) {
		this.endLimitCharCount = endLimitCharCount;
	}
	
	public String GetTitle(String html) {
		String title = "";
		Matcher match = Pattern.compile("<title>([^<>]*)</title>", Pattern.MULTILINE).matcher(html);
		if (match.find()) {
			title = match.group(1);
		}
		// 正文的标题一般在h1中，比title中的标题更干净
		match = Pattern.compile("<h1.*>(.*)</h1>", Pattern.MULTILINE).matcher(html);
		if (match.find()) {
			String h1 = match.group(1);
			if (h1 != null && h1.length() > 0 && title.startsWith(h1)) {
				title = h1;
			}
		}
		return title.trim();
	}
	
	public String GetContent(String html, boolean withTag) {
		//
		String[] orgLines = html.split("[\\n]"); // 保存原始内容，按行存储
		String[] lines = new String[orgLines.length]; // 保存干净的文本内容，不包含标签
		// 去除每行的空白字符,剔除标签
		for (int i = 0; i < orgLines.length; i++) {
			// 处理回车,制表符，使用-crlf-和-blta-做为标记符，最后统一处理
			lines[i] = orgLines[i].replaceAll("(?is)</p>|<br.*?/>|</tr>|</h\\d>", "-crlf-").replaceAll("(?is)</td>|</th>", "-blta-").replaceAll("(?is)<.*?>", "").trim();
		}
		StringBuilder sb = new StringBuilder();
		int preTextLen = 0; // 记录上一次统计的字符数量
		int startPos = -1; // 记录文章正文的起始位置
		for (int i = 0; i < lines.length - blocksWidth; i++) {
			int len = 0;
			for (int j = 0; j < blocksWidth; j++) {
				len += lines[i + j].length();
			}
			if (startPos == -1) // 还没有找到文章起始位置，需要判断起始位置
			{
				if (preTextLen > limitCount && len > 0) // 如果上次查找的文本数量超过了限定字数，且当前行数字符数不为0，则认为是开始位置
				{
					// 查找文章起始位置, 如果向上查找，发现_headEmptyLines行连续的空行则认为是头部
					for (int emptyCount = 0, j = i - 1; j > 0; j--) {
						if (lines[j] == null || lines[j].length() <= 0) {
							emptyCount++;
						}
						else {
							emptyCount = 0;
						}
						if (emptyCount == headEmptyLines) {
							startPos = j + headEmptyLines;
							break;
						}
					}
					// 如果没有定位到文章头，则以当前查找位置作为文章头
					if (startPos == -1) {
						startPos = i;
					}
					// 填充发现的文章起始部分
					if (withTag) {
						for (int j = startPos; j <= i; j++) {
							sb.append(orgLines[j]);
						}
					}
					else {
						for (int j = startPos; j <= i; j++) {
							sb.append(lines[j]);
						}
					}
				}
			}
			else {
				if (len <= endLimitCharCount && preTextLen < endLimitCharCount) // 当前长度为0，且上一个长度也为0，则认为已经结束
				{
					if (!appendMode) {
						break;
					}
					startPos = -1;
				}
				if (withTag) {
					sb.append(orgLines[i]);
				}
				else {
					sb.append(lines[i]);
				}
			}
			preTextLen = len;
		}
		//
		orgLines = null;
		lines = null;
		String result = sb.toString();
		sb = null;
		if (withTag) {// 输出带标签文本
			return result;
		}
		else {
			return CHtmlTrim.translateHTMLEscape(result.replaceAll("-blta-", "	").replaceAll("-crlf-", "\n"));
		}
	}
}
