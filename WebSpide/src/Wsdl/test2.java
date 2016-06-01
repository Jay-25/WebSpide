package Wsdl;

import org.apache.axis.types.Schema;
import org.w3c.dom.Element;

/**
 * @Title: main.java
 * @Package
 * @Description: TODO
 * @author
 * @date 2015-11-3 下午2:13:23
 * @version V1.0
 */
/**
 * @Copyright：2015
 * @Project：test1
 * @Description：
 * @Class：.main
 * @author：Zhao Jietong
 * @Create：2015-11-3 下午2:13:23
 * @version V1.0
 */
public class test2 {
	
	/**
	 * @Title: main
	 * @Description: TODO
	 * @param args
	 * @throws Exception
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
		CMyOption option = new CMyOption();
		option.setEndpointAddress("http://10.15.143.204:7308/WebServices4SLSM.asmx");
		// option.setEndpointAddress("https://180.169.10.229:31102/WebServices4SLSM.asmx");
		CMyWebservice Webservice = new CMyWebservice(option);
		CMyHTTPSTool.trustAllHttpsCertificates();
		// Schema result = Webservice.call("QueryZGRisk",
		// "SQLString","select abci.*, ub.UserID from vw_RVABCI abci, RVUserBAMapTable ub where abci.CompanyCode = ub.CompanyCode and abci.BAMapID = ub.BAMapID and length(ub.ReserveString)>0");
		Schema result = Webservice
		                .call("QueryZGRisk", "SQLString", "SELECT t.JobName, t.JobVersion, t.JobEnable, t.JobDesc, t.CompanyCode," + "\n" + "  (CASE WHEN t.RunTime IS NULL OR LENGTH(t.RunTime)=0 THEN dt.RunTime ELSE t.RunTime END) RunTime," + "\n" + "  (CASE WHEN t.endpointSelect IS NULL OR LENGTH(t.endpointSelect)=0 THEN dt.endpointSelect ELSE t.endpointSelect END) endpointSelect," + "\n" + "  (CASE WHEN t.namespaceURISelect IS NULL OR LENGTH(t.namespaceURISelect)=0 THEN dt.namespaceURISelect ELSE t.namespaceURISelect END) namespaceURISelect ," + "\n" + "  (CASE WHEN t.opNameSelect IS NULL OR LENGTH(t.opNameSelect)=0 THEN dt.opNameSelect ELSE t.opNameSelect END) opNameSelect ," + "\n" + "  (CASE WHEN t.opParasSelect IS NULL OR LENGTH(t.opParasSelect)=0 THEN dt.opParasSelect ELSE t.opParasSelect END) opParasSelect ," + "\n" + "  (CASE WHEN t.paramSelect IS NULL OR LENGTH(t.paramSelect)=0 THEN dt.paramSelect ELSE t.paramSelect END) paramSelect ," + "\n" + "  (CASE WHEN t.pathSelect IS NULL OR LENGTH(t.pathSelect)=0 THEN dt.pathSelect ELSE t.pathSelect END) pathSelect ," + "\n" + "  (CASE WHEN t.endpointInsert IS NULL OR LENGTH(t.endpointInsert)=0 THEN dt.endpointInsert ELSE t.endpointInsert END) ENDpointInsert ," + "\n" + "  (CASE WHEN t.namespaceURIInsert IS NULL OR LENGTH(t.namespaceURIInsert)=0 THEN dt.namespaceURIInsert ELSE t.namespaceURIInsert END) namespaceURIInsert ," + "\n" + "  (CASE WHEN t.opNameInsert IS NULL OR LENGTH(t.opNameInsert)=0 THEN dt.opNameInsert ELSE t.opNameInsert END) opNameInsert ," + "\n" + "  (CASE WHEN t.opParasInsert IS NULL OR LENGTH(t.opParasInsert)=0 THEN dt.opParasInsert ELSE t.opParasInsert END) opParasInsert ," + "\n" + "  (CASE WHEN t.paramInsert IS NULL OR LENGTH(t.paramInsert)=0 THEN dt.paramInsert ELSE t.paramInsert END) paramInsert ," + "\n" + "  (CASE WHEN t.pathInsert IS NULL OR LENGTH(t.pathInsert)=0 THEN dt.pathInsert ELSE t.pathInsert END) pathInsert ," + "\n" + "  (CASE WHEN t.endpointUpdate IS NULL OR LENGTH(t.endpointUpdate)=0 THEN dt.endpointUpdate ELSE t.endpointUpdate END) ENDpointUpdate ," + "\n" + "  (CASE WHEN t.namespaceURIUpdate IS NULL OR LENGTH(t.namespaceURIUpdate)=0 THEN dt.namespaceURIUpdate ELSE t.namespaceURIUpdate END) namespaceURIUpdate ," + "\n" + "  (CASE WHEN t.opNameUpdate IS NULL OR LENGTH(t.opNameUpdate)=0 THEN dt.opNameUpdate ELSE t.opNameUpdate END) opNameUpdate ," + "\n" + "  (CASE WHEN t.opParasUpdate IS NULL OR LENGTH(t.opParasUpdate)=0 THEN dt.opParasUpdate ELSE t.opParasUpdate END) opParasUpdate ," + "\n" + "  (CASE WHEN t.paramUpdate IS NULL OR LENGTH(t.paramUpdate)=0 THEN dt.paramUpdate ELSE t.paramUpdate END) paramUpdate ," + "\n" + "  (CASE WHEN t.pathUpdate IS NULL OR LENGTH(t.pathUpdate)=0 THEN dt.pathUpdate ELSE t.pathUpdate END) pathUpdate ," + "\n" + "  (CASE WHEN t.endpointDelete IS NULL OR LENGTH(t.endpointDelete)=0 THEN dt.endpointDelete ELSE t.endpointDelete END) ENDpointDelete ," + "\n" + "  (CASE WHEN t.namespaceURIDelete IS NULL OR LENGTH(t.namespaceURIDelete)=0 THEN dt.namespaceURIDelete ELSE t.namespaceURIDelete END) namespaceURIDelete ," + "\n" + "  (CASE WHEN t.opNameDelete IS NULL OR LENGTH(t.opNameDelete)=0 THEN dt.opNameDelete ELSE t.opNameDelete END) opNameDelete ," + "\n" + "  (CASE WHEN t.opParasDelete IS NULL OR LENGTH(t.opParasDelete)=0 THEN dt.opParasDelete ELSE t.opParasDelete END) opParasDelete ," + "\n" + "  (CASE WHEN t.paramDelete IS NULL OR LENGTH(t.paramDelete)=0 THEN dt.paramDelete ELSE t.paramDelete END) paramDelete ," + "\n" + "  (CASE WHEN t.pathDelete IS NULL OR LENGTH(t.pathDelete)=0 THEN dt.pathDelete ELSE t.pathDelete END) pathDelete ," + "\n" + "  (CASE WHEN t.logPath IS NULL OR LENGTH(t.logPath)=0 THEN dt.logPath ELSE t.logPath END) logPath ," + "\n" + "  (CASE WHEN t.smtp IS NULL OR LENGTH(t.smtp)=0 THEN dt.smtp ELSE t.smtp END) smtp ," + "\n" + "  (CASE WHEN t.username IS NULL OR LENGTH(t.username)=0 THEN dt.username ELSE t.username END) username ," + "\n" + "  (CASE WHEN t.password IS NULL OR LENGTH(t.password)=0 THEN dt.password ELSE t.password END) password ," + "\n" + "  (CASE WHEN t.emailSrc IS NULL OR LENGTH(t.emailSrc)=0 THEN dt.emailSrc ELSE t.emailSrc END) emailSrc ," + "\n" + "  (CASE WHEN t.emailTo IS NULL OR LENGTH(t.emailTo)=0 THEN dt.emailTo ELSE t.emailTo END) emailTo" + "\n" + "FROM TSyncTab t," + "\n" + "     (SELECT * FROM TSyncTab t1 WHERE t1.CompanyCode IS NULL) dt" + "\n" + "WHERE t.JobName = dt.JobName" + "\n" + "  AND t.CompanyCode IS NOT NULL" + "\n" + "  AND t.JobEnable = 1");
		Element rr = CMyWebservice.find(result.get_any()[1], "diffgram/NewDataSet/");
		CMyWebservice.foreach(result.get_any()[1], new CMyWebservice.ICallback() {
			
			@Override
			public Object callback(Element element) {
				CMyWebservice.print(element);
				System.out.println("---");
				return null;
			}
		});
	}
}
