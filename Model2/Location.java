package project;
/**
 * 
 * @author YAN
 * Not used in the final database (not enough data with location information.)
 */
public class Location {
	private double latitude;
	private double longitude;
	private double radius;
	private String name;

	public Location(double lat, double lon, double r, String n) {
		latitude = lat;
		longitude = lon;
		radius = r;
		name = n;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public double getRadius() {
		return radius;
	}

	public String getName() {
		return name;
	}

}
