import Extract.RegexEngine.CRegexEngine;
import RegexEngine.CRegexLib;

public class Test2 {
	
	public static void main(String[] args) {
		String str = "马水良 发布的 长沙-高鑫 麓城 房源 急售 岳麓一小学位 沁园春御院高鑫麓城 宜居 阳光 售价:90 万元 首付:27万 月供:3637元 户型:4室2厅2卫 相似户型 单价:6429元 面积:140平米 年代:2009年 朝向:南北 楼层:上部/11层 小区:高鑫 麓城(在售146套) 分享 虚假举报 关注房源 直接预约看房 马水良 13027316201 所属门店:御院分行A组 查询编号:SXA004052 房源编号:10284615   高鑫 麓城 小区介绍 高鑫 麓城 高鑫 麓城 本小区共有 租房146套 二手房146套 建筑年代: 建筑面积: 所在版块:岳麓区-桐梓坡路 总户数: 容积率: 绿化率: 物业费用: 物业公司: 物业类型:公寓 普通住宅 开发商:  ";
		CRegexEngine.TestRegex(str, CRegexLib.addressRegex("所在版块:", ""));
		CRegexEngine.TestRegex(str, CRegexLib.addressRegex("", ""));
	}
}
