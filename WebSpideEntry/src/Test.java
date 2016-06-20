public class Test {
	
	public static void main(String[] args) {
		try {
			@SuppressWarnings("unused")
			Double[] aaDoubles = new Double[1024000000];
			System.out.println(222);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("---");
		}
	}
}
