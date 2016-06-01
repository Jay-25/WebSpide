package Extract.RegexEngine;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * @Copyright�?014
 * @Project：Spide
 * @Description�? * @Class：Extract.RegexEngine.CRegex
 * @author：Zhao Jietong
 * @Create�?014-9-18 上午9:06:48
 * @version V1.0
 */
public class CRegex {
	
	// 后期处理
	public static interface IDeal {
		
		public String deal(String data);
	}
	
	// 深度解析
	public static interface IDeepSelect {
		
		public String select(String oldData, String newData);
	}
	
	private String                   format     = null;
	private int                      size       = 0;
	private String                   seperate   = " ";
	private IDeal                    dataDeal   = null;
	private IDeepSelect              deepSelect = null;
	private final String             regex;
	private final Pattern            pattern;
	private final ArrayList<Integer> idxs;
	
	public CRegex(String regex) {
		this(Pattern.MULTILINE, regex, 1);
	}
	
	public CRegex(String regex, Integer... idx) {
		this(Pattern.MULTILINE, regex, idx);
	}
	
	public CRegex(int patternValue, String regex, Integer... idx) {
		this.regex = regex;
		pattern = Pattern.compile(regex, patternValue);
		this.idxs = new ArrayList<Integer>();
		for (Integer id : idx) {
			idxs.add(id);
		}
	}
	
	@Override
	protected void finalize() throws Throwable {
		idxs.clear();
		super.finalize();
	};
	
	public CRegex setSize(int size) {
		this.size = size;
		return this;
	}

	public int getSize() {
		return size;
	}

	public String getSeperate() {
		return seperate;
	}

	public void setSeperate(String seperate) {
		this.seperate = seperate;
	}

	public String getRegex() {
		return regex;
	}
	
	public ArrayList<Integer> getIdx() {
		return idxs;
	}
	
	public Pattern getPattern() {
		return pattern;
	}
	
	public CRegex setFormat(final String blank) {
		this.format = blank;
		return this;
	}
	
	public String getFormat() {
		return format;
	}
	
	public IDeal getDataDeal() {
		return dataDeal;
	}
	
	public CRegex setDataDeal(IDeal dataDeal) {
		this.dataDeal = dataDeal;
		return this;
	}
	
	public IDeepSelect getDeepSelect() {
		return deepSelect;
	}
	
	public CRegex setDeepSelect(IDeepSelect deepSelect) {
		this.deepSelect = deepSelect;
		return this;
	}
}
