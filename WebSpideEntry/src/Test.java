import java.util.concurrent.atomic.AtomicLong;

public class Test {

	public static void main(String[] args) {
		AtomicLong jobNum = new AtomicLong(0);
		jobNum.incrementAndGet();
		System.out.println(jobNum.get());
		jobNum.decrementAndGet();
		System.out.println(jobNum.get());
	}
}
