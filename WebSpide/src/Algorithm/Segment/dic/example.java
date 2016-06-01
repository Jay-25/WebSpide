/**
 * @Title: example.java
 * @Package Algorithm.Segment.dic
 * @Description: TODO
 * @author
 * @date 2016-5-31 上午10:55:43
 * @version V1.0
 */
package Algorithm.Segment.dic;

import java.util.List;

import org.ansj.domain.Term;

/**
 * @Copyright：2016
 * @Project：WebSpide
 * @Description：
 * @Class：Algorithm.Segment.dic.example
 * @author：Zhao Jietong
 * @Create：2016-5-31 上午10:55:43
 * @version V1.0
 */

public class example {

	/**
	 * @Title: main
	 * @Description: TODO
	 * @param args
	 */
	public static void main(String[] args) {
		CSegment.getInstance().loadCustomDic();
		List<Term> terms = CSegment.parse("马水良 发布的 长沙-高鑫 麓城 房源 急售 岳麓一小学位");
		System.out.println(terms);

	}

}
