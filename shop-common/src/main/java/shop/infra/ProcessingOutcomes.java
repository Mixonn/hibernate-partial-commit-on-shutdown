package shop.infra;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/** Records how the processing of each message ended, so the test can check it. */
@Component
public class ProcessingOutcomes {

    private final BlockingQueue<Outcome> outcomes = new LinkedBlockingQueue<>();

    public void record(Runnable processing) {
        try {
            processing.run();
            outcomes.add(Outcome.success());
        } catch (RuntimeException e) {
            outcomes.add(Outcome.failure(e));
            throw e;
        }
    }

    public Outcome awaitNext() throws InterruptedException {
        Outcome outcome = outcomes.poll(90, TimeUnit.SECONDS);
        if (outcome == null) {
            throw new IllegalStateException("no message was processed");
        }
        return outcome;
    }

    public record Outcome(boolean succeeded, Throwable failure) {

        static Outcome success() {
            return new Outcome(true, null);
        }

        static Outcome failure(Throwable failure) {
            return new Outcome(false, failure);
        }

        /** Whether an exception of this class is anywhere in the cause chain (Spring and Hibernate wrap exceptions). */
        public boolean causedBy(String simpleClassName) {
            for (Throwable t = failure; t != null; t = t.getCause()) {
                if (t.getClass().getSimpleName().equals(simpleClassName)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            if (succeeded) {
                return "success";
            }
            Throwable root = failure;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            return "error: " + root.getClass().getSimpleName() + " (" + root.getMessage() + ")";
        }
    }
}
