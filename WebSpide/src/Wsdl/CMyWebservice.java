package Wsdl;

import java.net.URL;
import java.rmi.RemoteException;
import java.util.HashMap;

import javax.xml.namespace.QName;
import javax.xml.rpc.ParameterMode;
import javax.xml.rpc.ServiceException;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.axis.encoding.XMLType;
import org.apache.axis.types.Schema;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class CMyWebservice {

	private CMyOption option = null;

	private Service   service;
	private URL       url;

	public static interface ICallback {

		public Object callback(Element element);
	}

	public CMyWebservice(CMyOption option) throws Exception {
		this.option = option;
		service = new Service();
		url = new URL(option.endpointAddress);
		CMyHTTPSTool.trustAllHttpsCertificates();
	}

	@Override
	public CMyWebservice clone() {
		CMyWebservice ws = null;
		try {
			ws = new CMyWebservice(option);
			websHashMap.put(option.endpointAddress, ws);
		}
		catch (Exception e) {
		}

		return ws;
	}

	private static HashMap<String, CMyWebservice> websHashMap = new HashMap<String, CMyWebservice>();

	static public CMyWebservice getWebservice(CMyOption option) throws Exception {
		CMyWebservice ws = websHashMap.get(option.endpointAddress);
		if (ws != null) return ws;
		ws = new CMyWebservice(option);
		websHashMap.put(option.endpointAddress, ws);
		return ws;
	}

	@Override
	protected void finalize() throws java.lang.Throwable {
		service = null;
		url = null;

		super.finalize();
	}

	public String getEndpointAddress() {
		return option.endpointAddress;
	}

	public Schema call(String api, String para, String data) throws ServiceException, RemoteException {
		return call(api, new String[] { para }, new String[] { data });
	}

	public Schema call(String api, String[] paras, String[] datas) throws ServiceException, RemoteException {
		Call call = (Call) service.createCall();
		call.setEncodingStyle(option.encode);
		call.setTargetEndpointAddress(url);
		call.setOperationName(new QName(option.namespaceURI, api));
		for (int i = 0; i < paras.length; i++) {
			call.addParameter(new QName(option.namespaceURI, paras[i]), XMLType.XSD_STRING, ParameterMode.IN);
		}
		call.setReturnType(XMLType.XSD_SCHEMA);
		return (Schema) call.invoke(datas);
	}

	public static Element find(final Element element) {
		return find(element, "");
	}

	public static Element find(final Element element, String path) {
		if (element == null) return null;

		if (path.length() == 0) {
			return element;
		}

		int pos = path.indexOf('/');
		if (pos < 0) {
			path = path + "/";
			pos = path.length() - 1;
		}
		if (path.substring(0, pos).equals(element.getLocalName()) || path.substring(0, pos).equals(element.getNodeName())) {
			path = path.substring(pos + 1);
			pos = path.indexOf('/');
			if (path.length() == 0) {
				return element;
			}

			if (pos < 0) {
				path = path + "/";
				pos = path.length() - 1;
			}

			NodeList nodes = element.getChildNodes();
			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				if (node.getNodeType() == Element.ELEMENT_NODE) {
					if (path.substring(0, pos).equals(node.getLocalName()) || path.substring(0, pos).equals(node.getNodeName())) {
						if (node.hasChildNodes() && node.getFirstChild().getNodeType() == Element.ELEMENT_NODE) {
							return find((Element) node, path.substring(pos + 1));
						}
						else {
							return (Element) node;
						}
					}
				}
			}
		}

		return null;
	}

	public static int foreach(final Element element, ICallback calbak) {
		int counter = 0;
		if (element != null) {
			NodeList nodes = element.getChildNodes();
			if (nodes != null) {
				for (int i = 0; i < nodes.getLength(); i++) {
					Node node = nodes.item(i);
					if (node.getNodeType() == Element.ELEMENT_NODE) {
						counter++;
						if (calbak != null) calbak.callback((Element) node);
					}
				}
			}
		}

		return counter;
	}

	public static void print(final Node element) {
		print(element, 0);
		System.out.println("------------------------------");
	}

	private static void print(final Node element, int preBlank) {
		if (element == null) return;
		String blank = "";
		for (int i = 0; i < preBlank; i++) {
			blank += " ";
		}
		//
		if (element.hasChildNodes() && element.getFirstChild().getNodeType() == Element.TEXT_NODE) {
			System.out.println(blank + element.getNodeName() + " : " + element.getFirstChild().getNodeValue());
		}

		NodeList nodes = element.getChildNodes();
		if (nodes != null) {
			for (int i = 0; i < nodes.getLength(); i++) {
				print(nodes.item(i), preBlank + 4);
			}
		}
	}
}
