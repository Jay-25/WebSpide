package RegexEngine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import Algorithm.Math.CExpression;
import Extract.RegexEngine.CRegex;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Extract.RegexEngine.CDefaultRegex
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:43
 * @version V1.0
 */
public class CRegexLib {
	
	@SuppressWarnings("deprecation")
	public static List<CRegex> dateTimeRegex(String prefix, String suffix) {
		Date nowDate = new Date();
		List<CRegex> regexs = new ArrayList<CRegex>();
		// 2014年10月8日17:15:45
		regexs.add(new CRegex(prefix + "(20\\d{2}|\\d{2})[年/-]{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分:]([0-5]?\\d{1})[秒刻]?" + suffix, 1, 2, 3, 4, 5, 6)
		                .setFormat("${1}-${2}-${3} ${4}:${5}:${6}"));
		// 2014年10月8日17:15
		regexs.add(new CRegex(prefix + "(20\\d{2}|\\d{2})[年/-]{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分]?" + suffix, 1, 2, 3, 4, 5)
		                .setFormat("${1}-${2}-${3} ${4}:${5}:00"));
		// 2014年10月8日
		regexs.add(new CRegex(prefix + "(20\\d{2}|\\d{2})[年/-]{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?" + suffix, 1, 2, 3)
		                .setFormat("${1}-${2}-${3} 00:00:00"));
		// 10月8日17:15:45
		regexs.add(new CRegex(prefix + (prefix.length() == 0 ? "^" : "") + "{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分:]([0-5]?\\d{1})[秒刻]?" + suffix, 1, 2, 3, 4, 5)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} ${3}:${4}:${5}"));
		regexs.add(new CRegex(prefix + "[^0-9]+{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分:]([0-5]?\\d{1})[秒刻]?" + suffix, 1, 2, 3, 4, 5)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} ${3}:${4}:${5}"));
		// 10月8日17:15
		regexs.add(new CRegex(prefix + (prefix.length() == 0 ? "^" : "") + "{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分]?" + suffix, 1, 2, 3, 4)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} ${3}:${4}:00"));
		regexs.add(new CRegex(prefix + "[^0-9]+{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?[  	]*([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分]?" + suffix, 1, 2, 3, 4)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} ${3}:${4}:00"));
		// 10月8日
		regexs.add(new CRegex(prefix + (prefix.length() == 0 ? "^" : "") + "{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?" + suffix, 1, 2)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} 00:00:00"));
		regexs.add(new CRegex(prefix + "[^0-9]+{1}(0?\\d{1}|1[0-2])[月/-]{1}([0-2]?\\d{1}|30|31)[日号]?" + suffix, 1, 2)
		                .setFormat((nowDate.getYear() + 1900) + "-${1}-${2} 00:00:00"));
		// 17:15:45
		regexs.add(new CRegex(prefix + "([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分:]([0-5]?\\d{1})[秒刻]?" + suffix, 1, 2, 3)
		                .setFormat((nowDate.getYear() + 1900) + "-" + (nowDate.getMonth() + 1) + "-" + nowDate
		                                .getDate() + " ${1}:${2}:${3}"));
		// 17:15
		regexs.add(new CRegex(prefix + "([0-1]?\\d{1}|2[0-3])[时点:]([0-5]?\\d{1})[分]?" + suffix, 1, 2)
		                .setFormat((nowDate.getYear() + 1900) + "-" + (nowDate.getMonth() + 1) + "-" + nowDate
		                                .getDate() + " ${1}:${2}:00"));
		return regexs;
	}
	
	public static List<CRegex> dateTimeOffsetRegex(String prefix, String suffix) {
		class CDealDateTime implements CRegex.IDeal {
			
			@Override
			public String deal(String data) {
				try {
					Calendar rightNow = Calendar.getInstance();
					rightNow.setTime(new Date());
					rightNow.add(Calendar.SECOND, (Integer) CExpression.getExpresser()
					                .calculate(data));
					return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(rightNow.getTime());
				}
				catch (Exception e) {
				}
				return data;
			}
		}
		CDealDateTime dealDateTime = new CDealDateTime();
		List<CRegex> regexs = new ArrayList<CRegex>();
		// 10秒钟前
		regexs.add(new CRegex(prefix + "(\\d+)秒钟?前" + suffix, 1).setFormat("-${1}")
		                .setDataDeal(dealDateTime));
		// 10分钟前
		regexs.add(new CRegex(prefix + "(\\d+)分钟前" + suffix, 1).setFormat("-${1}*60")
		                .setDataDeal(dealDateTime));
		// 10小时前
		regexs.add(new CRegex(prefix + "(\\d+)小时前" + suffix, 1).setFormat("-${1}*60*60")
		                .setDataDeal(dealDateTime));
		// 10天前
		regexs.add(new CRegex(prefix + "(\\d+)天前" + suffix, 1).setFormat("-${1}*60*60*24")
		                .setDataDeal(dealDateTime));
		// 昨天
		regexs.add(new CRegex(prefix + "(昨天)" + suffix, 1).setFormat("-60*60*24")
		                .setDataDeal(dealDateTime));
		// 前天
		regexs.add(new CRegex(prefix + "(前天)" + suffix, 1).setFormat("-2*60*60*24")
		                .setDataDeal(dealDateTime));
		// 10周前
		regexs.add(new CRegex(prefix + "(\\d+)(周|星期)前" + suffix, 1).setFormat("-${1}*60*60*24*7")
		                .setDataDeal(dealDateTime));
		// 10秒钟后
		regexs.add(new CRegex(prefix + "(\\d+)秒钟?后" + suffix, 1).setFormat("${1}")
		                .setDataDeal(dealDateTime));
		// 10分钟后
		regexs.add(new CRegex(prefix + "(\\d+)分钟后" + suffix, 1).setFormat("${1}*60")
		                .setDataDeal(dealDateTime));
		// 10小时后
		regexs.add(new CRegex(prefix + "(\\d+)小时后" + suffix, 1).setFormat("${1}*60*60")
		                .setDataDeal(dealDateTime));
		// 10天后
		regexs.add(new CRegex(prefix + "(\\d+)天后" + suffix, 1).setFormat("${1}*60*60*24")
		                .setDataDeal(dealDateTime));
		// 明天
		regexs.add(new CRegex(prefix + "(明天)" + suffix, 1).setFormat("60*60*24")
		                .setDataDeal(dealDateTime));
		// 后天
		regexs.add(new CRegex(prefix + "(后天)" + suffix, 1).setFormat("2*60*60*24")
		                .setDataDeal(dealDateTime));
		// 10周后
		regexs.add(new CRegex(prefix + "(\\d+)(周|星期)后" + suffix, 1).setFormat("${1}*60*60*24*7")
		                .setDataDeal(dealDateTime));
		return regexs;
	}
	
	//
	public static List<CRegex> IPRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(((\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])\\.){3}(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*]))" + suffix));
		return regexs;
	}
	
	public static List<CRegex> URLRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "((https?|ftp|file)://[-a-zA-Z0-9\\+&@#/\\%\\?=~_\\|!:,\\.;]+)" + suffix));
		return regexs;
	}
	
	//
	public static List<CRegex> emailRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(([a-zA-Z0-9]*[-_]?[a-zA-Z0-9]+)*@([a-zA-Z0-9]*[-_]?[a-zA-Z0-9]+)+[\\.][A-Za-z]{2,3}([\\.][A-Za-z]{2})?)" + suffix));
		return regexs;
	}
	
	public static List<CRegex> telephoneRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "((0\\d{2,3}-)?\\d{7,8})" + suffix));
		return regexs;
	}
	
	public static List<CRegex> mobilePhoneRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "([1]([3][0-9]{1}|59|58|88|89)[0-9]{8})" + suffix));
		return regexs;
	}
	
	//
	public static List<CRegex> houseFaceRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(((东|南|西|北|阳|双|三)+[ ]*(东|南|西|北|阳|双|三)*)*)" + suffix));
		return regexs;
	}
	
	public static List<CRegex> houseStyleRegex(String prefix, String suffix) {
		final String keywords = "房|室|卧|厅|厨|卫|阳台";
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "((([0-9]+|[零一二三四五六七八九]+)(" + keywords + ")[ ]*)+)" + suffix)
		                .setDeepSelect(new CDeepSelect(keywords)).setDataDeal(new CRegex.IDeal() {
			                
			                @Override
			                public String deal(String data) {
				                return data.replaceAll("零", "0").replaceAll("一", "1")
				                                .replaceAll("二", "2").replaceAll("三", "3")
				                                .replaceAll("四", "4").replaceAll("五", "5")
				                                .replaceAll("六", "6")
				                                .replaceAll("七", "7").replaceAll("八", "8")
				                                .replaceAll("九", "9");
			                }
		                }));
		return regexs;
	}
	
	public static List<CRegex> houseDecorationRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(((未装修|清水|毛坯|简单装修|简装|普通装修|普装|中档装修|中等装修|中装|高档装修|时尚装修|老式装修|豪华装修|豪装|精装))+)" + suffix));
		return regexs;
	}
	
	public static List<CRegex> houseClassRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(((别墅|公房|经济适用房|商品房|商住两用|拆迁|安置|车库|高档|工业厂房|公寓|豪华住宅|豪宅|老公房|老洋房|平房|普通|商铺|商业|商住楼|四合院|写字楼|洋房|住宅))+)" + suffix));
		return regexs;
	}
	
	public static List<CRegex> buildClassRegex(String prefix, String suffix) {
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + "(((板楼|塔楼|平房))+)" + suffix));
		return regexs;
	}
	
	public static List<CRegex> addressRegex(String prefix, String suffix) {
		// 中文         \u4e00-\u9fa5
		// 全角数字 \uff10-\uff19
		// 全角英文 \uff21-\uff3a\uff41-\uff5a
		final String keywords = " |省|市|县|区|乡|镇|村|圃|屯|旗|盟|州|路口|路|郡|街|道口|道|小区|区|支|弄|巷|里|站|栋|幢|号|座|楼|府|公寓|大厦|公司|银行|医院|酒吧|小学|中学|大学|超市|大队|世界|中心|单元|校|层|库|池|社|会|厂|团|寺|场|园|苑|馆|局|部|室|所|城|店|院|公里|米|地块|旁边|附近|斜|对面|东|西|南|北|前|后|左|右|内|侧|交叉口|交口|交界处|交汇处|河边";
		final String reg = "(((?<=\\b|\\s|\\pP)[\u4e00-\u9fa5]++[\uff10-\uff19\uff21-\uff3a\uff41-\uff5a,-]*.*?(?<=" + keywords + "\b))+)";
		List<CRegex> regexs = new ArrayList<CRegex>();
		regexs.add(new CRegex(prefix + reg + suffix, 1).setSize(50)
		                .setDeepSelect(new CDeepSelect(keywords)));
		return regexs;
	}
}

class CDeepSelect implements CRegex.IDeepSelect {
	
	//private static Pattern pattenOut = Pattern.compile("(?is)(\\pP[t|b|z|r|d|p|c|e|y|o|h|k][a-z]*)|(\\pP[u](?!serDefine)[a-z]*)");
	private static double avgAddLen = 25;
	private String[]      ws        = null;
	
	public CDeepSelect(String keywords) {
		ws = keywords.split("\\|");
	}
	
	@Override
	public String select(String oldData, String newData) {
		double oldws = 0.0, newws = 0.0;
		for (int i = 0; i < ws.length; i++) {
			if (ws[i].equals(" ")) continue;
			double threshold = ws[i].length() * (1.0 - (double) i / ws.length);
			if (oldData.indexOf(ws[i]) != -1) {
				oldws += threshold;
			}
			if (newData.indexOf(ws[i]) != -1) {
				newws += threshold;
			}
		}
		double deltaOld = 1.0 / (1 + Math.abs(avgAddLen - oldData.length()));
		double deltaNew = 1.0 / (1 + Math.abs(avgAddLen - newData.length()));
		oldws = oldData.length() > 0 ? (deltaOld * oldws / oldData.length()) : 0.0;
		newws = newData.length() > 0 ? (deltaNew * newws / newData.length()) : 0.0;
		//System.out.println(newData + ":" + newws + "\n" + oldData + ":" + oldws);
		return newws > oldws ? newData : oldData;
	}
}
