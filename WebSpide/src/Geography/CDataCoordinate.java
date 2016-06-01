package Geography;


/**
 * @Copyright：2014
 * @Project：Spide
 * @Description：
 * @Class：Geography.CDataCoordinate
 * @author：Zhao Jietong
 * @Create：2014-9-18 上午9:06:57
 * @version V1.0
 */
public class CDataCoordinate {

	private double longitude; // 经度 + E, - W
	private double latitude; // 纬度 + N, - S
	private double elevation; // 海拔
	private String position; // 地点名称
	private String address;  // 地址
	private String postcode; // 邮政编码
	private String style;

	public CDataCoordinate() {
		setLongitude(0.0);
		setLatitude(0.0);
		setElevation(0.0);
		setAddress("");
		setPostcode("");
		setPosition("");
		setStyle("");
	}

	public void print() {
		System.out.printf("%s (%f, %f) %s %s [%s]\n", this.getStyle(), this.getLatitude(), this.getLongitude(), this.getPosition(), this.getAddress(), this.getPostcode());
	}

	/**
	 * @return the longitude
	 */
	public double getLongitude() {
		return longitude;
	}

	/**
	 * @param longitude
	 *            the longitude to set
	 */
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	/**
	 * @return the latitude
	 */
	public double getLatitude() {
		return latitude;
	}

	/**
	 * @param latitude
	 *            the latitude to set
	 */
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	/**
	 * @return the elevation
	 */
	public double getElevation() {
		return elevation;
	}

	/**
	 * @param elevation
	 *            the elevation to set
	 */
	public void setElevation(double elevation) {
		this.elevation = elevation;
	}

	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * @param address
	 *            the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}

	/**
	 * @return the postcode
	 */
	public String getPostcode() {
		return postcode;
	}

	/**
	 * @param postcode
	 *            the postcode to set
	 */
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	/**
	 * @return position
	 */
	public String getPosition() {
		return position;
	}

	/**
	 * @param position
	 *            要设置的 position
	 */
	public void setPosition(String position) {
		this.position = position;
	}

	/**
	 * @return style
	 */
	public String getStyle() {
		return style;
	}

	/**
	 * @param style
	 *            要设置的 style
	 */
	public void setStyle(String style) {
		this.style = style;
	}
}
