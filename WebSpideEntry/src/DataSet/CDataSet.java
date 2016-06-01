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

public class CDataSet {

	public static CSpideDataStruct createDataSet(String job_name) {
		CSpideDataStruct dataSet = new CSpideDataStruct() {

			@Override
			public boolean isValidData() {
				if (!super.isValidData()) return false;
				if (isNull(getData("price")) || (Double) getData("price") <= 0) return false;
				if (isNull(getData("address")) || ((String) getData("address")).length() >= 128) return false;
				return true;
			}
		};
		// 作业名称
		dataSet.defineColumn("job_name", "String", false, job_name);
		// 站内标识
		dataSet.defineColumn("web_in_uid", "String", false, null);
		// 发布时间
		dataSet.defineColumn("release_time", "String", false, CDateTime.getCurrentTime("yyyy-MM-dd"));
		// 总价
		dataSet.defineColumn("price", "Double", false, null);
		// 单价
		dataSet.defineColumn("price_unit", "Double", false, null);
		// 产权
		dataSet.defineColumn("house_property", "String", true, null);
		// 户型
		dataSet.defineColumn("house_style", "String", true, null);
		// 结构
		dataSet.defineColumn("house_struct", "String", true, null);
		// 装修
		dataSet.defineColumn("house_decoration", "String", true, null);
		// 住宅类别
		dataSet.defineColumn("house_class", "String", true, null);
		// 建筑类型
		dataSet.defineColumn("build_class", "String", true, null);
		// 楼盘名称
		dataSet.defineColumn("build_name", "String", true, null);
		// 使用面积
		dataSet.defineColumn("usage_area", "Double", true, null);
		// 建筑面积
		dataSet.defineColumn("build_area", "Double", true, null);
		// 年代
		dataSet.defineColumn("build_time_year", "String", true, null);
		// 朝向
		dataSet.defineColumn("build_face", "String", true, null);
		// 楼层
		dataSet.defineColumn("build_layer", "String", true, null);
		// 总楼层
		dataSet.defineColumn("build_max_layer", "String", true, null);
		// 地址-小区
		dataSet.defineColumn("address_city", "String", true, null);
		// 地址
		dataSet.defineColumn("address", "String", false, null);
		// 开发商
		dataSet.defineColumn("develop_company", "String", true, null);
		// 物业费
		dataSet.defineColumn("property_costs", "String", true, null);
		// 物业公司
		dataSet.defineColumn("property_company", "String", true, null);
		// 按揭首付
		dataSet.defineColumn("mortgage_down_payment", "String", true, null);
		// 月供
		dataSet.defineColumn("monthly", "String", true, null);
		// 信息来源
		dataSet.defineColumn("url", "String", false, null);
		// 网址来源
		dataSet.defineColumn("url_cofrom", "String", false, null);
		// 地理位置来源
		dataSet.defineColumn("style", "String", false, null);
		// 经度 + E, - W
		dataSet.defineColumn("longitude", "Double", false, null);
		// 纬度 + N, - S
		dataSet.defineColumn("latitude", "Double", false, null);
		// 海拔
		dataSet.defineColumn("elevation", "Double", true, null);
		// 原始信息
		dataSet.defineColumn("raw", "String", false, null);
		//
		dataSet.clear();
		return dataSet;
	}
}
