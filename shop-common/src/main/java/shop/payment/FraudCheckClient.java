package shop.payment;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * An external fraud-check system. In a real application this would be an HTTP call; here the test decides when
 * the system "answers". By default it answers immediately.
 */
@Component
public class FraudCheckClient {

    private final CountDownLatch requestArrived = new CountDownLatch(1);
    private final CountDownLatch answer = new CountDownLatch(1);
    private volatile boolean slow;

    public void verify(String orderId) {
        requestArrived.countDown();
        if (slow) {
            waitForAnswer();
        }
    }

    // --- controlled by the test ---

    public void respondSlowly() {
        slow = true;
    }

    public void awaitRequest() throws InterruptedException {
        if (!requestArrived.await(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the handler never called the fraud check");
        }
    }

    public void respondNow() {
        answer.countDown();
    }

    /** Like waiting for a real HTTP response: interrupting the thread does not stop it. */
    private void waitForAnswer() {
        boolean interrupted = false;
        while (answer.getCount() > 0) {
            try {
                answer.await();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
