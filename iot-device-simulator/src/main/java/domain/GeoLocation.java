package domain;

import static util.Utils.getRandomIntegerInRange;

public class GeoLocation {

    private int latitude;
    private int longitude;

    public GeoLocation(int latitude, int longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public int getLatitude() {
        return latitude;
    }

    public void setLatitude(int latitude) {
        this.latitude = latitude;
    }

    public int getLongitude() {
        return longitude;
    }

    public void setLongitude(int longitude) {
        this.longitude = longitude;
    }

    public static GeoLocation randomLocation() {
        return new GeoLocation(getRandomIntegerInRange(-90,90), getRandomIntegerInRange(-180,180));
    }

}
