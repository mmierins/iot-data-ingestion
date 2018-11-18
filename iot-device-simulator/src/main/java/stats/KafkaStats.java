package stats;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaStats {

    private AtomicLong success = new AtomicLong();
    private AtomicLong failure = new AtomicLong();
    private AtomicLong attempt = new AtomicLong();

    public void incrementSuccess() {
        success.incrementAndGet();
    }

    public void incrementFailure() {
        failure.incrementAndGet();
    }

    public void incrementAttempt() {
        attempt.incrementAndGet();
    }

    public long getSuccess() {
        return success.get();
    }

    public long getFailure() {
        return failure.get();
    }

    public long getAttempt() {
        return attempt.get();
    }

}
