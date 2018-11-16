package domain;

public class DataEnvelope {

    private TemperatureDeviceData data;

    public DataEnvelope(TemperatureDeviceData data) {
        this.data = data;
    }

    public TemperatureDeviceData getData() {
        return data;
    }

    public void setData(TemperatureDeviceData data) {
        this.data = data;
    }

}
