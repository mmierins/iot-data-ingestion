package domain;

public class TemperatureDeviceData {

    private String deviceId;
    private GeoLocation location;
    private int temperature;
    private long time;

    public String getDeviceId() {
        return deviceId;
    }

    public TemperatureDeviceData(String deviceId, GeoLocation location, int temperature, long time) {
        this.deviceId = deviceId;
        this.location = location;
        this.temperature = temperature;
        this.time = time;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public GeoLocation getLocation() {
        return location;
    }

    public void setLocation(GeoLocation location) {
        this.location = location;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
