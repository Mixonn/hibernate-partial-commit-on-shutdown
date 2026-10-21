package shop;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;

/**
 * Records how each transaction ended, as the application saw it, so the test can check it. Spring Boot attaches
 * this listener to the transaction manager on its own.
 */
class TransactionOutcomes implements TransactionExecutionListener {

    private final BlockingQueue<Outcome> outcomes = new LinkedBlockingQueue<>();

    @Override
    public void afterCommit(TransactionExecution transaction, Throwable commitFailure) {
        outcomes.add(new Outcome("COMMIT", commitFailure));
    }

    @Override
    public void afterRollback(TransactionExecution transaction, Throwable rollbackFailure) {
        outcomes.add(new Outcome("ROLLBACK", rollbackFailure));
    }

    Outcome awaitNext() throws InterruptedException {
        Outcome outcome = outcomes.poll(90, TimeUnit.SECONDS);
        if (outcome == null) {
            throw new IllegalStateException("no transaction ended");
        }
        return outcome;
    }

    /** How the transaction ended, COMMIT or ROLLBACK, and the error, if that step itself failed. */
    record Outcome(String ending, Throwable failure) {

        boolean committed() {
            return ending.equals("COMMIT") && failure == null;
        }

        boolean rolledBack() {
            return ending.equals("ROLLBACK") && failure == null;
        }

        @Override
        public String toString() {
            if (committed()) {
                return "the transaction was committed";
            }
            if (rolledBack()) {
                return "the transaction was rolled back";
            }
            return ending + " failed: " + failure;
        }
    }
}
