package shop.window;

import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceUtils;
import shop.payment.FraudCheckClient;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * The fraud-check stand-in for the measurement. During warm-up it answers right away. For the measured payment it
 * keeps the transaction open, the way a slow HTTP call would, and answers a set time after a cue: Hibernate has been
 * closed, or Spring has logged that it is closing Hibernate.
 * <p>
 * It spins instead of sleeping, so it answers within a microsecond or so of the chosen moment. Interrupting the
 * thread does not stop it, just as it does not stop a thread waiting for an HTTP response.
 */
@Primary
class TimedFraudCheck extends FraudCheckClient {

    private final SessionFactory hibernate;
    private final DataSource dataSource;
    private final CountDownLatch requestArrived = new CountDownLatch(1);
    private volatile BooleanSupplier cue;
    private volatile long delayNanos;
    private volatile Connection connection;
    private volatile long answeredAt = ShutdownStopwatch.NOT_SEEN;
    private volatile Thread paymentThread;

    TimedFraudCheck(EntityManagerFactory hibernate, DataSource dataSource) {
        this.hibernate = hibernate.unwrap(SessionFactory.class);
        this.dataSource = dataSource;
    }

    /**
     * The next payment waits for the cue, then for the delay, and only then gets its answer.
     */
    void answerNextPaymentAfter(BooleanSupplier cue, long delayNanos) {
        this.delayNanos = delayNanos;
        this.cue = cue;
    }

    BooleanSupplier hibernateClosed() {
        return () -> !hibernate.isOpen();
    }

    @Override
    public void verify(String orderId) {
        BooleanSupplier cue = this.cue;
        if (cue == null) {
            return;
        }
        this.cue = null;
        connection = physicalConnectionOfThisTransaction();
        paymentThread = Thread.currentThread();
        requestArrived.countDown();

        long giveUp = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while (!cue.getAsBoolean() && System.nanoTime() < giveUp) {
            Thread.onSpinWait();
        }
        long cueSeen = System.nanoTime();
        while (System.nanoTime() - cueSeen < delayNanos) {
            Thread.onSpinWait();
        }
        answeredAt = System.nanoTime();
    }

    /**
     * Waits until the measured payment is in the middle of its transaction and returns that transaction's JDBC
     * connection.
     */
    Connection awaitRequest() throws InterruptedException {
        if (!requestArrived.await(60, TimeUnit.SECONDS)) {
            throw new IllegalStateException("the payment service never called the fraud check");
        }
        return connection;
    }

    /**
     * The thread recording the measured payment. Spring shuts its executor down when the pod stops, so the thread ends
     * as soon as the payment is over.
     */
    Thread paymentThread() {
        return paymentThread;
    }

    /**
     * When the fraud check answered (System.nanoTime()); from here the payment service goes straight to the commit.
     */
    long answeredAt() {
        return answeredAt;
    }

    /**
     * The JDBC connection under the current transaction, taken out of HikariCP's wrapper, so that the stopwatch can see
     * the moment the pool closes it.
     */
    private Connection physicalConnectionOfThisTransaction() {
        Connection pooled = DataSourceUtils.getConnection(dataSource);
        try {
            return pooled.unwrap(Connection.class);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        } finally {
            DataSourceUtils.releaseConnection(pooled, dataSource);
        }
    }
}
