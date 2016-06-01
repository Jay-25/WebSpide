package Algorithm.Kmeans;

import java.util.Date;

public class _kmeans_example {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// double[][] points3 = { { 0, 4, 3 }, { 4, 6, 9 }, { 1, 2, 99 }, { 8, 56, 7 } }; //
		// 测试数据，四个二维的点
		double[][] points = { { 5.0 }, { 2.0 }, { 2.0 }, { 4.0 }, { 7.0 }, { 4.0 }, { 15.0 }, { 4.0 }, { 4.0 }, { 4.0 }, { 3.0 }, { 9.0 }, { 10.0 }, { 4.0 }, { 6.0 }, { 6.0 }, { 18.0 }, { 11.0 }, { 6.0 }, { 12.0 }, { 19.0 }, { 2.0 }, { 5.0 }, { 2.0 },
		                { 15.0 }, { 108.0 }, { 4.0 }, { 4.0 }, { 4.0 }, { 18.0 }, { 15.0 }, { 16.0 }, { 19.0 }, { 11.0 }, { 17.0 }, { 18.0 }, { 9.0 }, { 2.0 } };
		long t0 = new Date().getTime();
		int[] results = new CKmeans().loadData(points).toKmeans(3, 0.1, 1000);
		System.out.println("::" + (new Date().getTime() - t0));
		for (int r : results) {
			System.out.print(r + "  ");
		}
	}
}
