package Algorithm.Math;

import java.util.ArrayList;
import java.util.List;

public class CExpression {

	private final static String CHAE_L = "(";
	private final static String CHAE_R = ")";
	private final static String ADD    = "+";
	private final static String MIN    = "-";
	private final static String MUL    = "*";
	private final static String DIV    = "/";
	private final static String MOD    = "%";
	private static CExpression  exper  = new CExpression();

	private CExpression() {
	}

	public static CExpression getExpresser() {
		return exper;
	}

	public static void main(String arges[]) {
		CExpression exper = CExpression.getExpresser();
		try {
			System.out.println((-2 * 3) + 6 - 4.0 / 6);
			System.out.println(exper.calculate("(-2*3)+6-4.0/6"));
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * calculate result
	 * 
	 * @param expStr
	 * @return
	 * @throws Exception
	 */
	public Object calculate(String expStr) throws Exception {
		// 1. init and check expression str
		expStr = initAndCheck(expStr);
		// 2. get calculate item
		ItemCalculate item = new ItemCalculate(null, expStr);
		item.genCalculateItem();
		// 3. calculate
		item.calculate();
		return item.getResult();
	}

	private class ItemCalculate {

		public ItemCalculate(String sign, String expStr) {
			this.sign = sign;
			this.expStr = expStr;
		}

		private String              sign;
		private final String        expStr;
		private List<ItemCalculate> sonItemList = null;
		private Object              result      = null;

		private boolean isInteger() {
			if (result != null) {
				if (result instanceof Double) {
					return false;
				}
				else {
					return true;
				}
			}
			if (expStr.indexOf(".") > 0) return false;
			return true;
		}

		public void genCalculateItem() throws Exception {
			int index = 0;
			String lastSign = ADD;
			for (int i = 0; i < expStr.length(); i++) {
				String one = expStr.substring(i, i + 1);
				if (isSign(one)) {// before
					if (i == 0) {
						lastSign = one;
						index = i + 1;
					}
					else {
						String numStr = expStr.substring(index, i);
						this.addItem(lastSign, numStr);
						lastSign = one;
						index = i + 1;
					}
				}
				else if (isLkuohao(one)) {// after
					int oKuohaoIndex = getOtherKuohaoIndex(expStr, i);
					if (oKuohaoIndex < 0) throw new Exception("expression str error1, '(',')' not matching.");
					String sonExpStr = expStr.substring(index + 1, oKuohaoIndex);
					ItemCalculate item = this.addItem(lastSign, sonExpStr);
					item.genCalculateItem();
					if (oKuohaoIndex >= expStr.length() - 1) return;
					lastSign = expStr.substring(oKuohaoIndex + 1, oKuohaoIndex + 2);
					index = oKuohaoIndex + 2;
					i = index - 1;
				}
				else if (i == expStr.length() - 1) {
					String numStr = expStr.substring(index, i + 1);
					this.addItem(lastSign, numStr);
				}
			}
		}

		private int getOtherKuohaoIndex(String expStr, int lkuoHaoIndex) {
			int rnum = 0;
			int lnum = 0;
			for (int i = lkuoHaoIndex + 1; i < expStr.length(); i++) {
				String one = expStr.substring(i, i + 1);
				if (isLkuohao(one)) {
					lnum++;
				}
				else if (isRkuohao(one)) {
					if (lnum == rnum) return i;
					rnum++;
				}
			}
			return -1;
		}

		public void calculate() throws Exception {
			if (sonItemList == null) {
				if (!isInteger()) {
					setResult(getDouble(getExpStr()));
				}
				else {
					setResult(getInteger(getExpStr()));
				}
				return;
			}
			for (int i = 0; i < sonItemList.size(); i++) {
				ItemCalculate sonItem = sonItemList.get(i);
				sonItem.calculate();
			}
			ItemCalculate lastItem = null;
			for (int i = 0; i < sonItemList.size(); i++) {
				if (sonItemList.size() < 2) break;
				ItemCalculate sonItem = sonItemList.get(i);
				String sign = sonItem.getSign();
				if (isHightLeavl(sign) && lastItem != null) {
					Integer lRsti = null;
					Double lRstd = null;
					Integer sRsti = null;
					Double sRstd = null;
					if (lastItem.isInteger()) lRsti = (Integer) lastItem.getResult();
					else
						lRstd = (Double) lastItem.getResult();
					if (sonItem.isInteger()) sRsti = (Integer) sonItem.getResult();
					else
						sRstd = (Double) sonItem.getResult();
					if (MUL.equals(sign)) {
						if (lRsti != null && sRsti != null) {
							lastItem.setResult(lRsti * sRsti);
						}
						else if (lRsti != null && sRstd != null) {
							lastItem.setResult(lRsti * sRstd);
						}
						else if (lRstd != null && sRsti != null) {
							lastItem.setResult(lRstd * sRsti);
						}
						else if (lRstd != null && sRstd != null) {
							lastItem.setResult(lRstd * sRstd);
						}
					}
					else if (DIV.equals(sign)) {
						if (lRsti != null && sRsti != null) {
							lastItem.setResult(lRsti / sRsti);
						}
						else if (lRsti != null && sRstd != null) {
							lastItem.setResult(lRsti / sRstd);
						}
						else if (lRstd != null && sRsti != null) {
							lastItem.setResult(lRstd / sRsti);
						}
						else if (lRstd != null && sRstd != null) {
							lastItem.setResult(lRstd / sRstd);
						}
					}
					else if (MOD.equals(sign)) {
						if (lRsti != null && sRsti != null) {
							lastItem.setResult(lRsti % sRsti);
						}
						else if (lRsti != null && sRstd != null) {
							lastItem.setResult(lRsti % sRstd);
						}
						else if (lRstd != null && sRsti != null) {
							lastItem.setResult(lRstd % sRsti);
						}
						else if (lRstd != null && sRstd != null) {
							lastItem.setResult(lRstd % sRstd);
						}
					}
					sonItemList.remove(i);
					i = -1;
					lastItem = null;
					continue;
				}
				lastItem = sonItem;
			}
			lastItem = null;
			for (int i = 0; i < sonItemList.size(); i++) {
				if (sonItemList.size() < 2) break;
				ItemCalculate sonItem = sonItemList.get(i);
				if (i == 0) {
					lastItem = sonItem;
					continue;
				}
				String sign = sonItem.getSign();
				if (lastItem != null) {
					Integer lRsti = null;
					Double lRstd = null;
					Integer sRsti = null;
					Double sRstd = null;
					if (lastItem.isInteger()) lRsti = (Integer) lastItem.getResult();
					else
						lRstd = (Double) lastItem.getResult();
					if (sonItem.isInteger()) sRsti = (Integer) sonItem.getResult();
					else
						sRstd = (Double) sonItem.getResult();
					if (MIN.equals(lastItem.getSign())) {
						if (lRsti != null) lRsti = -lRsti;
						else if (lRstd != null) lRstd = -lRstd;
						lastItem.setSign(ADD);
					}
					if (ADD.equals(sign)) {
						if (lRsti != null && sRsti != null) {
							lastItem.setResult(lRsti + sRsti);
						}
						else if (lRsti != null && sRstd != null) {
							lastItem.setResult(lRsti + sRstd);
						}
						else if (lRstd != null && sRsti != null) {
							lastItem.setResult(lRstd + sRsti);
						}
						else if (lRstd != null && sRstd != null) {
							lastItem.setResult(lRstd + sRstd);
						}
					}
					else if (MIN.equals(sign)) {
						if (lRsti != null && sRsti != null) {
							lastItem.setResult(lRsti - sRsti);
						}
						else if (lRsti != null && sRstd != null) {
							lastItem.setResult(lRsti - sRstd);
						}
						else if (lRstd != null && sRsti != null) {
							lastItem.setResult(lRstd - sRsti);
						}
						else if (lRstd != null && sRstd != null) {
							lastItem.setResult(lRstd - sRstd);
						}
					}
					sonItemList.remove(i);
					i = -1;
					lastItem = null;
					continue;
				}
			}
			ItemCalculate item = sonItemList.get(0);
			if (MIN.equals(item.getSign())) {
				if (item.isInteger()) {
					Integer res = getInteger(item.getResult());
					setResult(-res);
				}
				else {
					Double res = getDouble(item.getResult());
					setResult(-res);
				}
			}
			else
				setResult(item.getResult());
		}

		public ItemCalculate addItem(String sign, String expStr) {
			ItemCalculate item = new ItemCalculate(sign, expStr);
			if (sonItemList == null) sonItemList = new ArrayList<ItemCalculate>();
			sonItemList.add(item);
			return item;
		}

		public String getExpStr() {
			return expStr;
		}

		public String getSign() {
			return sign;
		}

		public void setSign(String sign) {
			this.sign = sign;
		}

		public Object getResult() {
			return result;
		}

		public void setResult(Object result) {
			this.result = result;
		}
	}

	/**
	 * init and check expression string
	 * 
	 * @param expStr
	 * @return
	 * @throws Exception
	 */
	private String initAndCheck(String expStr) throws Exception {
		if (expStr == null || expStr.trim().equals("")) throw new Exception("expression str error, is empty.");
		// to upper case
		expStr = expStr.toUpperCase();
		// remove " " string
		expStr = expStr.replaceAll(" ", "");
		// check "(",")"
		int lnum = 0;
		int rnum = 0;
		boolean lastIsSign = false;
		for (int i = 0; i < expStr.length(); i++) {
			String one = expStr.substring(i, i + 1);
			if (CHAE_L.equals(one)) lnum++;
			if (CHAE_R.equals(one)) rnum++;
			if (isSign(one)) {
				if (lastIsSign) throw new Exception("expression str error, sign together.");
				lastIsSign = true;
			}
			else {
				lastIsSign = false;
			}
		}
		if (rnum != lnum) throw new Exception("expression str error, '(',')' not matching.");
		return expStr;
	}

	private Double getDouble(Object val) throws Exception {
		if (val == null) throw new Exception("expression str error, Double null.");
		return new Double(val.toString());
	}

	private Integer getInteger(Object val) throws Exception {
		if (val == null) throw new Exception("expression str error, Double null.");
		return new Integer(val.toString());
	}

	private boolean isHightLeavl(String val) {
		if (MUL.equals(val) || DIV.equals(val) || MOD.equals(val)) {
			return true;
		}
		return false;
	}

	private boolean isSign(String val) {
		if (ADD.equals(val) || MIN.equals(val) || MUL.equals(val) || DIV.equals(val) || MOD.equals(val)) {
			return true;
		}
		return false;
	}

	private boolean isLkuohao(String val) {
		if (CHAE_L.equals(val)) {
			return true;
		}
		return false;
	}

	private boolean isRkuohao(String val) {
		if (CHAE_R.equals(val)) {
			return true;
		}
		return false;
	}
}
