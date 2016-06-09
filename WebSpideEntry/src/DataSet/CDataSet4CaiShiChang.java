/**
 * @Title: CDataSet.java
 * @Package DataSet
 * @Description: TODO
 * @author
 * @date 2016-5-23 下午5:06:24
 * @version V1.0
 */
package DataSet;

import DateTime.CDateTime;
import SpiderBase.CSpideDataStruct;

/**
 * @Copyright：2016
 * @Project：WebSpideEntry
 * @Description：
 * @Class：DataSet.CDataSet
 * @author：Zhao Jietong
 * @Create：2016-5-23 下午5:06:24
 * @version V1.0
 */
public class CDataSet4CaiShiChang {
	
	public static CSpideDataStruct createDataSet(String job_name) {
		CSpideDataStruct dataSet = new CSpideDataStruct() {
			
			@Override
			public boolean isValidData() {
				return super.isValidData();
			}
		};
		// 0=菜价数据，1=营养数据，2=市场数据
		dataSet.defineColumn("dataStyle", "int", false, null);
		// 作业名称
		dataSet.defineColumn("job_name", "String", false, job_name);
		// 发布时间
		dataSet.defineColumn("updateTime", "String", false, CDateTime.getCurrentTime("yyyy-MM-dd"));
		// 市场
		dataSet.defineColumn("market", "String", false, null);
		// 类型
		dataSet.defineColumn("type", "String", false, null);
		// 名称
		dataSet.defineColumn("name", "String", false, null);
		// 均价
		dataSet.defineColumn("avgPrice", "Double", true, 0);
		// 最高价钱
		dataSet.defineColumn("maxPrice", "Double", true, 0);
		// 最低价钱
		dataSet.defineColumn("minPrice", "Double", true, 0);
		// 是否是均价
		dataSet.defineColumn("average", "String", true, "0");
		//
		//
		dataSet.defineColumn("food", "String", false, null);
		dataSet.defineColumn("unit", "String", false, null);
		dataSet.defineColumn("val", "Double", false, null);
		dataSet.defineColumn("nrv", "int", false, null);
		dataSet.defineColumn("per", "String", false, null);
		//
		//
		dataSet.defineColumn("district", "String", false, null);
		dataSet.defineColumn("updated", "String", false, "0");
		//
		dataSet.clear();
		return dataSet;
	}
}
