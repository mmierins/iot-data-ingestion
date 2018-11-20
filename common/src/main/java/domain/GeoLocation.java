package domain;

import static util.Utils.getRandomDoubleInRange;

public class GeoLocation {

    private String latitude;
    private String longitude;

    public GeoLocation(String latitude, String longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public static GeoLocation randomLocation() {
        return new GeoLocation(String.valueOf(getRandomDoubleInRange(-90,90)), String.valueOf(getRandomDoubleInRange(-180,180)));
    }

    @Override
    public String toString() {
        return "GeoLocation{" +
                "latitude='" + latitude + '\'' +
                ", longitude='" + longitude + '\'' +
                '}';
    }
}
