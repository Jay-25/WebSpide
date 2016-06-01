package Wsdl;

import java.io.IOException;
import java.util.Hashtable;

import javax.net.ssl.SSLSession;

import org.apache.axis.AxisProperties;
import org.apache.axis.components.net.JSSESocketFactory;

public class CMyHTTPSTool extends JSSESocketFactory {

	public static javax.net.ssl.SSLContext       sslContext       = null;
	public static javax.net.ssl.HostnameVerifier hostnameVerifier = null;

	static {
		int tryNum = 4;
		while (tryNum-- > 0) {
			try {
				/**
				 * 域名验证器
				 */
				CMyHTTPSTool.hostnameVerifier = new javax.net.ssl.HostnameVerifier() {

					@Override
					public boolean verify(String urlHostName, SSLSession session) {
						// ip address of the service URL(like.23.28.244.244)
						// return urlHostName.equals("23.28.244.244");
						System.out.println("Warning: URL Host: " + urlHostName + " vs. " + session.getPeerHost());
						return true;
					}
				};

				// Create a trust manager that does not validate certificate chains
				javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[] { new javax.net.ssl.X509TrustManager() {

					@Override
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					@Override
					public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) throws java.security.cert.CertificateException {
						return;
					}

					@Override
					public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) throws java.security.cert.CertificateException {
						return;
					}
				} };

				// Install the all-trusting trust manager
				CMyHTTPSTool.sslContext = javax.net.ssl.SSLContext.getInstance("SSL");
				CMyHTTPSTool.sslContext.init(null, trustAllCerts, null);
				// MyHTTPSTool.sslContext.init(null, trustAllCerts, new
				// java.security.SecureRandom());
				break;
			}
			catch (Exception e) {
			}
			//
			try {
				Thread.sleep(20 * 1000);
			}
			catch (Exception ex) {
			}
		}
	}

	@SuppressWarnings("rawtypes")
	public CMyHTTPSTool(Hashtable attributes) {
		super(attributes);
	}

	@Override
	protected void initFactory() throws IOException {
		super.initFactory();
		this.sslFactory = CMyHTTPSTool.sslContext.getSocketFactory();
	}

	/**
	 * @throws Exception
	 */
	public static void trustAllHttpsCertificates() throws Exception {
		javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(CMyHTTPSTool.hostnameVerifier); // 设置默认的域名验证器
		javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(CMyHTTPSTool.sslContext.getSocketFactory()); // 设置默认的Socket工厂
		// 指定Axis类库要调用的SocketFactory。用来自定义进行HTTPS验证
		AxisProperties.setProperty("axis.socketSecureFactory", CMyHTTPSTool.class.getName());
	}
}
