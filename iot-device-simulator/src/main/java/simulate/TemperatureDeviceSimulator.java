package simulate;

import domain.DataEnvelope;
import domain.GeoLocation;
import domain.TemperatureDeviceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import stats.KafkaStats;

import java.time.Instant;
import java.util.UUID;

import static util.Utils.getRandomIntegerInRange;

@Component
@Scope("prototype")
public class TemperatureDeviceSimulator {

    private static final Logger log = LoggerFactory.getLogger(TemperatureDeviceSimulator.class);

    private boolean isInit = false;
    private KafkaTemplate<String, DataEnvelope> kafkaTemplate;
    private String kafkaTopic;
    private KafkaStats kafkaStats;

    // these variables are "static" per simulation session per device
    private String deviceId;
    private GeoLocation location;

    private DataEnvelope dataEnvelope;

    public DataEnvelope getDataEnvelope() {
        return dataEnvelope;
    }

    @Scheduled(fixedRateString = "${simulation.frequency}")
    public void simulate() {
        if (!isInit) {
            deviceId = UUID.randomUUID().toString();
            location = GeoLocation.randomLocation();

            log.info("Created simulated temperature device {}", this);
            isInit = true;
        }


        TemperatureDeviceData data = new TemperatureDeviceData(deviceId, location, getRandomIntegerInRange(-100, 100), Instant.now().toEpochMilli() / 1000);
        dataEnvelope = new DataEnvelope(data);

        kafkaStats.incrementAttempt();

        ListenableFuture<SendResult<String, DataEnvelope>> future = kafkaTemplate.send(kafkaTopic, dataEnvelope);
        future.addCallback(new ListenableFutureCallback<SendResult<String, DataEnvelope>>() {

            @Override
            public void onFailure(Throwable throwable) {
                log.error("Could not send message " + dataEnvelope, throwable);
                kafkaStats.incrementFailure();
            }

            @Override
            public void onSuccess(SendResult<String, DataEnvelope> sendResult) {
                kafkaStats.incrementSuccess();
            }

        });
    }

    @Value("${kafka.topic}")
    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    @Autowired
    public void setKafkaStats(KafkaStats kafkaStats) {
        this.kafkaStats = kafkaStats;
    }

    @Autowired
    public void setKafkaTemplate(KafkaTemplate<String, DataEnvelope> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String toString() {
        return "TemperatureDeviceSimulator{" +
                "deviceId='" + deviceId + '\'' +
                ", location=" + location +
                '}';
    }
}
