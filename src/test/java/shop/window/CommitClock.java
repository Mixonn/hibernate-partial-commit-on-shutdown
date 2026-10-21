package shop.window;

import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Notes when Spring starts and finishes committing the measured payment, and how it ended as the application saw it.
 * Spring Boot attaches this listener to the transaction manager on its own.
 */
class CommitClock implements TransactionExecutionListener {

    private final CountDownLatch over = new CountDownLatch(1);
    private volatile boolean watching;
    private volatile long started = ShutdownStopwatch.NOT_SEEN;
    private volatile long ended = ShutdownStopwatch.NOT_SEEN;
    private volatile String ending;
    private volatile Throwable failure;

    /**
     * Only the transaction that starts after this call is measured; the warm-up ones before it are not.
     */
    void watchNextTransaction() {
        watching = true;
    }

    @Override
    public void beforeCommit(TransactionExecution transaction) {
        if (watching && started == ShutdownStopwatch.NOT_SEEN) {
            started = System.nanoTime();
        }
    }

    @Override
    public void afterCommit(TransactionExecution transaction, Throwable commitFailure) {
        end("COMMIT", commitFailure);
    }

    @Override
    public void afterRollback(TransactionExecution transaction, Throwable rollbackFailure) {
        end("ROLLBACK", rollbackFailure);
    }

    private void end(String how, Throwable failure) {
        if (watching && over.getCount() > 0) {
            ended = System.nanoTime();
            ending = how;
            this.failure = failure;
            over.countDown();
        }
    }

    void awaitEnd() throws InterruptedException {
        if (!over.await(2, TimeUnit.MINUTES)) {
            throw new IllegalStateException("the measured transaction never ended");
        }
    }

    long started() {
        return started;
    }

    long ended() {
        return ended;
    }

    /**
     * COMMIT or ROLLBACK.
     */
    String ending() {
        return ending;
    }

    /**
     * The error of the commit or rollback itself, or null if it went through.
     */
    Throwable failure() {
        return failure;
    }
}
