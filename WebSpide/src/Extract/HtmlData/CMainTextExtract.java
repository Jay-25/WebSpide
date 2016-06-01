package Extract.HtmlData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import Algorithm.Kmeans.CKmeans;
import Extract.Reduce.CHtmlTrim;

/**
 * <p>
 * 在线性时间内抽取主题类（新闻、博客等）网页的正文。 采用了<b>基于行块分布函数</b>的方法，为保持通用性没有针对特定网站编写规则。
 * </p>
 * 
 * @author Chen Xin
 * @version 1.0, 2009-11-11
 */
public class CMainTextExtract {
	
	public static final long    MODE_HOLD_ALL                 = 0xFFFFFFFF;
	public static final long    MODE_HOLD_NUMBER              = 0x00000001;
	public static final long    MODE_HOLD_ENGLISH             = 0x00000002;
	public static final long    MODE_HOLD_CHINESE             = 0x00000004;
	public static final long    MODE_HOLD_DATE                = 0x00000008;
	public static final long    MODE_HOLD_NUMBER_SYMBOL       = 0x00000100;
	public static final long    MODE_HOLD_ENGLISH_SYMBOL      = 0x00000200;
	public static final long    MODE_HOLD_CHINESE_SYMBOL      = 0x00000400;
	public static final long    MODE_HOLD_OTHER_SYMBOL        = 0x01000000;
	public static final long    MODE_HOLD_MONEY_SYMBOL        = 0x02000000;
	public static final long    MODE_UNHOLD_URL               = 0x10000000;
	//
	private static final String MODE_HOLD_NUMBER_RULE         = "0-9\\.";
	private static final String MODE_HOLD_NUMBER_SYMBOL_RULE  = "\\+\\-\\*\\/\\.\\(\\)\\=\\{\\}\\[\\]\\<\\>\\?\\%";
	private static final String MODE_HOLD_DATE_RULE           = "0-9-/:";
	private static final String MODE_HOLD_ENGLISH_RULE        = "a-zA-Z";
	private static final String MODE_HOLD_ENGLISH_SYMBOL_RULE = "\\~\\!\\@\\#\\$\\%\\^\\&\\*\\(\\),\\.\\?\\/\\\\\"\\:\\;\\'";
	private static final String MODE_HOLD_CHINESE_RULE        = "\n\t\r\u4e00-\u9fa5";
	private static final String MODE_HOLD_CHINESE_SYMBOL_RULE = "：？；￥㎡m³－～［］（）｛｝【】〈〉《》·！＋－×，。、…∶＂＇｀｜〃「」『』〔〕 ";
	private static final String MODE_HOLD_MONEY_SYMBOL_RULE   = "￥￡＄￠€";
	private static final String MODE_HOLD_OTHER_SYMBOL_RULE   = "©®";
	private static final String MODE_UNHOLD_URL_RULE          = "(https?|ftp|file)://[-a-zA-Z0-9\\+&@#/\\%\\?=~_\\|!:,\\.;]+";
	//
	private final int           DEFAULT_THRESHOLD             = 86;
	private int                 blocksWidth_default;
	private int                 threshold_default;
	private long                mode;
	
	private class _Threshold {
		
		public double maxD       = 0;
		public int[]  pcount     = null;
		public int    _threshold = Integer.MAX_VALUE;
		
		public _Threshold(int K) {
			pcount = new int[K];
			for (int i = 0; i < K; i++) {
				pcount[i] = 0;
			}
		}
	}
	
	private class _UrlCatalog {
		
		public int praseTimes     = 0;
		public int garbageWordSum = 0;
	}
	
	private HashMap<String, _UrlCatalog> urlCatalogs = new HashMap<String, _UrlCatalog>();
	
	public CMainTextExtract() {
		blocksWidth_default = 3;
		mode = MODE_HOLD_ALL;
		/* 当待抽取的网页正文中遇到成块的新闻标题未剔除时，只要增大此阈值即可。 */
		/* 阈值增大，准确率提升，召回率下降；值变小，噪声会大，但可以保证抽到只有一句话的正文 */
		threshold_default = DEFAULT_THRESHOLD;
	}
	
	@Override
	protected void finalize() throws Throwable {
		urlCatalogs.clear();
		urlCatalogs = null;
		super.finalize();
	}
	
	public CMainTextExtract setThreshold(int value) {
		threshold_default = value;
		return this;
	}
	
	public CMainTextExtract setBlocksWidth(int value) {
		blocksWidth_default = value;
		return this;
	}
	
	public CMainTextExtract setMode(long m) {
		mode = m;
		return this;
	}
	
	public String parse(String html) {
		return parse("", html);
	}
	
	public String parse(String catalog, String html) {
		return getText(catalog, trimHtml(html));
	}
	
	private String trimHtml(String html) {
		html = html.replaceAll("(?is)<!DOCTYPE.*?>", "");
		html = html.replaceAll("(?is)<!--.*?-->", ""); // remove html comment
		html = html.replaceAll("(?is)<.*?>", "");
		html = html.replaceAll("(?is)<script.*?>.*?</script>", ""); // remove javascript
		html = html.replaceAll("(?is)<noscript.*?>.*?</noscript>", ""); // remove javascript
		html = html.replaceAll("(?is)<style.*?>.*?</style>", ""); // remove css
		html = CHtmlTrim.translateHTMLEscape(html);
		html = html.replaceAll("&.{2,5};|&#.{2,5};", " "); // remove special char
		html = html.replaceAll("[	 ]", " ");
		if ((mode & MODE_UNHOLD_URL) == MODE_UNHOLD_URL) {
			html = html.replaceAll(MODE_UNHOLD_URL_RULE, "");
		}
		String rule = "";
		if ((mode & MODE_HOLD_NUMBER) == MODE_HOLD_NUMBER) rule += MODE_HOLD_NUMBER_RULE;
		if ((mode & MODE_HOLD_ENGLISH) == MODE_HOLD_ENGLISH) rule += MODE_HOLD_ENGLISH_RULE;
		if ((mode & MODE_HOLD_CHINESE) == MODE_HOLD_CHINESE) rule += MODE_HOLD_CHINESE_RULE;
		if ((mode & MODE_HOLD_DATE) == MODE_HOLD_DATE) rule += MODE_HOLD_DATE_RULE;
		if ((mode & MODE_HOLD_NUMBER_SYMBOL) == MODE_HOLD_NUMBER_SYMBOL) rule += MODE_HOLD_NUMBER_SYMBOL_RULE;
		if ((mode & MODE_HOLD_ENGLISH_SYMBOL) == MODE_HOLD_ENGLISH_SYMBOL) rule += MODE_HOLD_ENGLISH_SYMBOL_RULE;
		if ((mode & MODE_HOLD_CHINESE_SYMBOL) == MODE_HOLD_CHINESE_SYMBOL) rule += MODE_HOLD_CHINESE_SYMBOL_RULE;
		if ((mode & MODE_HOLD_MONEY_SYMBOL) == MODE_HOLD_MONEY_SYMBOL) rule += MODE_HOLD_MONEY_SYMBOL_RULE;
		if ((mode & MODE_HOLD_OTHER_SYMBOL) == MODE_HOLD_OTHER_SYMBOL) rule += MODE_HOLD_OTHER_SYMBOL_RULE;
		if (rule.length() > 0) html = html.replaceAll("[^" + rule + "]", "");
		// System.out.println("**************");
		// System.out.println(html);
		// System.out.println("**************");
		return html;
	}
	
	/*
	 * 动态计算阈值
	 * @author Zhao Jietong
	 */
	private _Threshold calThreshold(int K, ArrayList<Integer> indexDistribution) {
		_Threshold TT = new _Threshold(K);
		int ilen = indexDistribution.size();
		// 确定非0数据的个数
		for (int i = ilen - 1; i >= 0; i--) {
			if (indexDistribution.get(i) == 0) ilen--;
		}
		if (ilen == 0) return TT;
		// 针对非0的数据进行Kmeans分类
		double[][] ipoints = new double[ilen][1];
		for (int j = 0, i = indexDistribution.size() - 1; i >= 0 && j < ilen; i--) {
			if (indexDistribution.get(i) != 0) {
				ipoints[j] = new double[1];
				ipoints[j++][0] = indexDistribution.get(i);
			}
		}
		CKmeans kmeans = new CKmeans();
		int[] kresults = kmeans.loadData(ipoints).toKmeans(K, 1, 20);
		// for (int r : kresults)System.out.print(r + "  ");
		// 根据分类，找出阈值
		int maxD_idx = 0;
		for (int i = 0; i < ilen; i++) {
			if (TT.maxD < ipoints[i][0]) {
				TT.maxD = ipoints[i][0];
				maxD_idx = i;
			}
		}
		// System.out.println(TT.maxD);
		// 统计每个分类下数据的数量，并排序
		for (int i = 0; i < kresults.length; i++) {
			TT.pcount[kresults[i]]++;
		}
		for (int i = 0; i < TT.pcount.length; i++) {
			for (int j = i + 1; j < TT.pcount.length; j++) {
				if (TT.pcount[i] > TT.pcount[j]) {
					int tmp = TT.pcount[j];
					TT.pcount[j] = TT.pcount[i];
					TT.pcount[i] = tmp;
				}
			}
		}
		// 计算除最大数据值意外的所有分类下的峰值，并找出最小的那个峰值，即阈值
		ArrayList<Integer> thresholds = new ArrayList<Integer>();
		for (int kk = 0; kk < K; kk++) {
			if (kk == maxD_idx) continue;
			int _threshold = 0;
			for (int i = 0; i < ilen; i++) {
				if (kresults[i] == kresults[kk]) {
					if (_threshold < ipoints[i][0]) {
						_threshold = (int) ipoints[i][0];
					}
				}
			}
			thresholds.add(_threshold);
		}
		int size = thresholds.size();
		for (int kk = 0; kk < size; kk++) {
			if (TT._threshold > thresholds.get(kk)) {
				TT._threshold = thresholds.get(kk);
			}
		}
		kmeans = null;
		ipoints = null;
		thresholds = null;
		return TT;
	}
	
	private String getText(String catalog, String html) {
		ArrayList<Integer> indexDistribution = new ArrayList<Integer>();
		List<String> lines = Arrays.asList(html.split("\n"));
		for (int i = lines.size() - 1; i >= 0; i--) {
			lines.set(i, lines.get(i).replaceAll("\\s{1,}", " ").trim()); // remove repeat blank
		}
		//
		_UrlCatalog urlCatalog;
		urlCatalog = urlCatalogs.get(catalog);
		if (urlCatalog == null) {
			urlCatalog = new _UrlCatalog();
			urlCatalogs.put(catalog, urlCatalog);
		}
		//
		int K = 3;
		boolean reBlock = false;
		int threshold_cal = threshold_default;
		int blocksWidth = blocksWidth_default;
		do {
			reBlock = false;
			{// 原始算法部分
				indexDistribution.clear();
				int ln = lines.size() - blocksWidth;
				for (int i = 0; i < ln; i++) {
					int wordsNum = 0;
					int lnb = i + blocksWidth;
					for (int j = i; j < lnb; j++) {
						for (int n = lines.get(j).length() - 1; n >= 0; n--) {
							if (lines.get(j).charAt(n) != ' ') wordsNum++;
						}
					}
					indexDistribution.add(wordsNum);
					// System.out.println(wordsNum);
				}
			}
			//
			_Threshold TT = calThreshold(K, indexDistribution);
			threshold_cal = (threshold_default != DEFAULT_THRESHOLD) ? threshold_default : TT._threshold;
			// for (int i = 0; i < TT.pcount.length; i++) {
			// System.out.print(TT.pcount[i] + ",");
			// }
			// System.out.println();
			// 根据Kmeans分类的统计结果进行比例运算，进而尝试调整K值或者blockWidth值
			double pr = (double) TT.pcount[TT.pcount.length - 2] / (double) TT.pcount[TT.pcount.length - 1];
			if (pr > 0.5) {
				if (K > 2) {
					K = 2;
					reBlock = true;
				}
				else if (blocksWidth > 1) {
					K = 3;
					reBlock = true;
					blocksWidth--;
				}
			}
			TT = null;
		} while (reBlock);
		/*
		 * 确定了大致的阈值后，抽取的信息中可能包含垃圾信息，也可能正文信息被忽略了一些。
		 * 这里，我假设，同一网站中，同一个栏目（由urlCatalog指定）下的所有网页中，垃圾信息的字节数是相对稳定的。
		 * 因此，假设之前的抽取都是正确的，从而可以计算垃圾信息的平均字节数，
		 * 那么，当采用上述阈值进行抽取时，可以对比其垃圾信息的字节数，不过超过允许范围（0.8~1.2）之间，就需要对 阈值进行尝试性的调整。
		 */
		String text = "";
		double threshold_d = 0.8;
		boolean attempForThreshold = false;
		do {
			int threshold = (int) (threshold_cal * threshold_d);
			{// 原始算法部分
				text = "";
				int start = -1;
				int end = -1;
				boolean boolstart = false;
				boolean boolend = false;
				int ilen = indexDistribution.size() - 3;
				for (int i = 0; i < ilen; i++) {
					if (indexDistribution.get(i) > threshold && !boolstart) {
						if (indexDistribution.get(i + 1) != 0 || indexDistribution.get(i + 2) != 0 || indexDistribution.get(i + 3) != 0) {
							boolstart = true;
							start = i;
							continue;
						}
					}
					if (boolstart) {
						if (indexDistribution.get(i) == 0 || indexDistribution.get(i + 1) == 0) {
							end = i;
							boolend = true;
						}
					}
					if (boolend) {
						for (int ii = start; ii <= end; ii++) {
							if (lines.get(ii).length() < 5) continue;
							text += lines.get(ii) + "\n";
						}
						boolstart = false;
						boolend = false;
					}
				}
			}
			if (urlCatalog.praseTimes > 0) {
				int avgGarbageWordSum = urlCatalog.garbageWordSum / urlCatalog.praseTimes;
				int curGarbageWordSum = html.length() - text.length();
				if (curGarbageWordSum < avgGarbageWordSum * 0.8) {
					threshold_d += 0.2;
					attempForThreshold = (threshold_d > 0) ? true : false;
				}
				else if (curGarbageWordSum > avgGarbageWordSum * 1.2) {
					threshold_d -= 0.2;
					attempForThreshold = (threshold_d > 0) ? true : false;
				}
				else {
					attempForThreshold = false;
				}
			}
			if (text.length() == 0) {
				threshold_d -= 0.3;
				attempForThreshold = (threshold_d > 0) ? true : false;
			}
			else {
				attempForThreshold = false;
			}
		} while (attempForThreshold);
		//
		urlCatalog.praseTimes++;
		urlCatalog.garbageWordSum += html.length() - text.length();
		//
		lines = null;
		indexDistribution = null;
		// System.out.println("\nINFO: Threshold:" + threshold + " for block " + blocksWidth);
		// System.out.println("garbageWords: " + (urlCatalog.garbageWordSum /
		// urlCatalog.praseTimes));
		return text.trim();
	}
}
