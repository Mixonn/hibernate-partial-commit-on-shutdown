package shop;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Primary;
import shop.payment.FraudCheckClient;

/**
 * The test's stand-in for the external fraud-check system: the test decides when it answers. By default it answers
 * right away.
 */
@Primary
class SlowFraudCheck extends FraudCheckClient {

    private final CountDownLatch requestArrived = new CountDownLatch(1);
    private final CountDownLatch answer = new CountDownLatch(1);
    private volatile boolean slow;

    @Override
    public void verify(String orderId) {
        requestArrived.countDown();
        if (slow) {
            waitForAnswer();
        }
    }

    void respondSlowly() {
        slow = true;
    }

    void awaitRequest() throws InterruptedException {
        if (!requestArrived.await(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the payment service never called the fraud check");
        }
    }

    void respondNow() {
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
