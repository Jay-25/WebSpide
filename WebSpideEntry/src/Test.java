public class Test {
	
	public static void main(String[] args) {
		try {
			@SuppressWarnings("unused")
			Double[] aaDoubles = null;
			aaDoubles = new Double[1024000000];
			System.out.println(aaDoubles.length);
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			System.out.println("---===");
		}
		catch (Throwable e) {
			System.out.println(e.getMessage());
			System.out.println("---");
		}
	}
}
