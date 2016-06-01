/**
 * @Title: CPdf2Text.java
 * @Package Convert.FileConvert
 * @Description: TODO
 * @author
 * @date 2015-5-4 下午2:09:04
 * @version V1.0
 */
package Convert.FileConvert;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**     
 * @Copyright：2015
 * @Project：Spide     
 *  
 * @Description：  
 * @Class：Convert.FileConvert.CPdf2Text       
 * @author：Zhao Jietong
 * @Create：2015-5-4 下午2:09:04     
 * @version   V1.0      
 */

public class CPdf2Text {
	
	private final String xpdfPath;
	private String       pdfFileName;
	private String       cmdParas;
	
	public CPdf2Text(String xpdfPath) {
		this.xpdfPath = xpdfPath;
	}
	
	public void setFile(String fileName) {
		this.pdfFileName = fileName;
	}
	
	public void setCmdParas(String cmdParas) {
		this.cmdParas = cmdParas;
	}
	
	private String execute(String cmd) {
		try {
			System.out.println(cmd);
			Process process = Runtime.getRuntime().exec(cmd);
			//String err = new BufferedReader(new InputStreamReader(process.getErrorStream())).readLine();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));//获得exe执行程序返回结果

			String result = "";
			String s = "";
			while ((s = br.readLine()) != null) {
				result += s + "\n";
			}

			return result; //执行结果
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	private String getCmd(String cmd, String... paras) {
		if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
			cmd = cmd + ".exe";
			for (String para : paras) {
				cmd += " " + para;
			}
			cmd = "cmd /c cd " + xpdfPath + " & " + cmd;
		}
		else {
			for (String para : paras) {
				cmd += " " + para;
			}
			cmd = "/bin/sh -c cd " + xpdfPath + " & " + cmd;
		}
		return cmd;
	}
	
	public String getPdfInfo() {
		String cmd = getCmd("pdfinfo", cmdParas, pdfFileName);
		return execute(cmd);
	}
	
	public String convertText(String txtFileName) {
		String cmd = getCmd("pdftotext", cmdParas, pdfFileName, txtFileName);
		return execute(cmd);
	}
	
	public String convertText() {
		return convertText("");
	}
}
