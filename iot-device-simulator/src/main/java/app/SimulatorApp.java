package app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import simulate.TemperatureDeviceSimulator;
import stats.KafkaStats;

@SpringBootApplication(scanBasePackages = { "app.config", "simulate", "stats" })
@EnableScheduling
public class SimulatorApp implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SimulatorApp.class);

    private ApplicationContext applicationContext;
    private int numDevices = 1;
    private KafkaStats kafkaStats;

    public static void main(String[] args) {
        SpringApplication.run(SimulatorApp.class, args);
    }

    @Override
    public void run(String... args) {
        initialize();
    }

    @Scheduled(fixedRate = 5000)
    public void printStats() {
        log.info("Kafka statistics: messages attempted: {}, succeeded: {}, failed: {}", kafkaStats.getAttempt(), kafkaStats.getSuccess(), kafkaStats.getFailure());
    }

    private void initialize() {
        for (int i = 0; i < numDevices; i++) {
            TemperatureDeviceSimulator bean = new TemperatureDeviceSimulator();

            AutowireCapableBeanFactory factory = applicationContext.getAutowireCapableBeanFactory();
            factory.autowireBean(bean);
            factory.initializeBean(bean, String.format("%s#%d", TemperatureDeviceSimulator.class.getSimpleName(), i));
        }
    }

    @Autowired
    public void setKafkaStats(KafkaStats kafkaStats) {
        this.kafkaStats = kafkaStats;
    }

    @Value("${num.devices}")
    public void setNumDevices(int numDevices) {
        this.numDevices = numDevices;
    }

    @Autowired
    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

}
