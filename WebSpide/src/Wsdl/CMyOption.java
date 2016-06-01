/**
 * @Title: CMyOption.java
 * @Package MyWsdl
 * @Description: TODO
 * @author
 * @date 2015-11-3 下午3:04:25
 * @version V1.0
 */
package Wsdl;

/**
 * @Copyright：2015
 * @Project：test1
 * @Description：
 * @Class：MyWsdl.CMyOption
 * @author：Zhao Jietong
 * @Create：2015-11-3 下午3:04:25
 * @version V1.0
 */

public class CMyOption {

	protected String namespaceURI    = "http://tempuri.org/";
	protected String endpointAddress = "";
	protected String encode          = "UTF-8";

	/**
	 * @param namespaceURI
	 *            the namespaceURI to set
	 */
	public void setNamespaceURI(String namespaceURI) {
		this.namespaceURI = namespaceURI;
	}

	/**
	 * @param endpointAddress
	 *            the endpointAddress to set
	 */
	public void setEndpointAddress(String endpointAddress) {
		this.endpointAddress = endpointAddress;
	}

	/**
	 * @param encode
	 *            the encode to set
	 */
	public void setEncode(String encode) {
		this.encode = encode;
	}

}
