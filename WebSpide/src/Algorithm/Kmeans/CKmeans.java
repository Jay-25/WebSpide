package Algorithm.Kmeans;

import java.util.Random;

/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Algorithm.Kmeans.CKmeans
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:11
 * @version V1.0
 */
public class CKmeans {

	private double[][] m_data;               // 数据，第一维表示数据数量，第二维表示N维向量
	private int        m_dataLength;         // 数据数量
	private int        m_dataDim;            // 数据维数
	private int[]      m_labels;             // 聚类后，数据所属的聚类标号(0~k-1)
	private double[][] m_centers;            // k个聚类中心点的坐标
	private double[][] m_centers4update;     // 为更新而用的中心点的坐标
	private double[][] m_centers4last;       // 上一次的中心点的坐标
	private int[]      m_centerCounts4update; // 每个中心分类所包含的点的数量
	private double[]   m_centerDis2;         // k个聚类中心点到各自分类的点的方差
	private int        m_k;                  // 分类数量

	/**
	 * 加载数据
	 * 
	 * @param data
	 * @return
	 */
	public CKmeans loadData(double[][] data) {
		m_data = data;
		m_dataLength = data.length;
		m_dataDim = data[0].length;
		return this;
	}

	/**
	 * 计算Kmeans
	 * 
	 * @param k
	 *            ：分类个数
	 * @param criteria
	 *            ：中心点最小移动距离
	 * @param attempts
	 *            ：尝试次数
	 * @return
	 */
	public int[] toKmeans(int k, double criteria, int attempts) {
		m_k = k;
		init();
		initCenter();
		double min_d2 = Double.MAX_VALUE;
		int[] result = new int[m_dataLength];
		int _attempts = attempts;
		while (_attempts-- > 0) {
			do {
				reset();
				for (int i = 0; i < m_dataLength; i++) {
					double minDist = distance(m_data[i], m_centers[0]);
					int label = 0;
					for (int j = 1; j < m_k; j++) {
						double tempDist = distance(m_data[i], m_centers[j]);
						if (tempDist < minDist) {
							minDist = tempDist;
							label = j;
						}
					}
					m_labels[i] = label;
					m_centerCounts4update[label]++;
					m_centerDis2[label] += minDist; // 累计数据点到中心的距离
				}
				updateCenters();
				// testPrint();
				// COutput.println("centers move: " + centerOffset());
			} while (centerOffset() > criteria);
			// 总和所有分类下的 “累计数据点到中心的距离”，取最小的一次作为最优解
			double cd2 = 0;
			for (int j = 0; j < m_k; j++) {
				if (m_centerCounts4update[j] != 0) {
					cd2 += m_centerDis2[j];
				}
				else { // 如果存在某分类下，没有所属数据点
					cd2 = Double.MAX_VALUE;
				}
			}
			if (min_d2 > cd2) {
				min_d2 = cd2;
				for (int i = 0; i < m_dataLength; i++) {
					result[i] = m_labels[i];
				}
				if (cd2 == 0) {
					break;
				}
			}
		}
		m_centers = null;
		m_centers4update = null;
		m_centers4last = null;
		m_centerCounts4update = null;
		m_centerDis2 = null;
		m_labels = null;
		return result;
	}

	/**
	 * 欧氏距离
	 * 
	 * @param point1
	 * @param point2
	 * @return
	 */
	protected double distance(double[] point1, double[] point2) {
		double rv = 0;
		for (int i = 0; i < m_dataDim; i++) {
			double temp = point1[i] - point2[i];
			rv += temp * temp;
		}
		return rv;
		// return Math.sqrt(rv);
	}

	/*
	 * 中心点偏移量的方差
	 */
	protected double centerOffset() {
		double d = 0;
		for (int i = 0; i < m_k; i++) {
			d += distance(m_centers4last[i], m_centers[i]);
		}
		return d;
	}

	protected void initCenter() {
		Random rn = new Random();
		for (int i = 0; i < m_k; i++) {
			for (int j = 0; j < m_dataDim; j++) {
				m_centers[i][j] = rn.nextDouble();
			}
		}
	}

	/*
	 * 初始化中心
	 */
	private void init() {
		m_labels = new int[m_dataLength];
		m_centers = new double[m_k][m_dataDim];
		m_centers4update = new double[m_k][m_dataDim];
		m_centers4last = new double[m_k][m_dataDim];
		m_centerCounts4update = new int[m_k];
		m_centerDis2 = new double[m_k];
	}

	/*
	 * 重置中心统计
	 */
	private void reset() {
		for (int j = 0; j < m_k; j++) {
			m_centerCounts4update[j] = 0;
			m_centerDis2[j] = 0;
		}
	}

	/*
	 * 备份中心点
	 */
	private void backupCenters() {
		for (int i = 0; i < m_k; i++) {
			for (int j = 0; j < m_dataDim; j++) {
				m_centers4last[i][j] = m_centers[i][j];
			}
		}
	}

	/*
	 * 更新中心点
	 */
	private void updateCenters() {
		// 备份中心点
		backupCenters();
		for (int i = 0; i < m_k; i++) {
			for (int j = 0; j < m_dataDim; j++) {
				m_centers4update[i][j] = 0;
			}
		}
		// 更新中心点
		for (int i = 0; i < m_dataDim; i++) {
			for (int j = 0; j < m_dataLength; j++) {
				m_centers4update[m_labels[j]][i] += m_data[j][i];
			}
			for (int t = 0; t < m_k; t++) {
				if (m_centerCounts4update[t] > 0) {
					m_centers[t][i] = m_centers4update[t][i] / m_centerCounts4update[t];
				}
			}
		}
	}

	/*
	 * 测试输出
	 */
	public void testPrint() {
		for (int i = 0; i < m_k; i++) {
			System.out.println("center: (" + m_centers[i][0] + ") " + m_centerCounts4update[i]);
		}
		System.out.println("The labels of points is: ");
		for (int i = 0; i < m_dataLength; i++) {
			System.out.print("(");
			System.out.print(m_data[i][0]);
			for (int j = 1; j < m_dataDim; j++) {
				System.out.print(", " + m_data[i][j]);
			}
			System.out.print(")=[ ");
			System.out.println(m_labels[i] + " ]");
		}
	}
}
