package Extract.Reduce;

public class ex_example {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String resAtts[] = { "rowspan", "colspan" };
		String elms[] = { "a", "p", "div", "strong", "span" };
		String result = "";
		result = CHtmlTrim.dropAttributes(result, resAtts);
		result = CHtmlTrim.dropSingleElement(result, elms);
	}
}
