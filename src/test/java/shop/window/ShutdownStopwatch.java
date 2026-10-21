package shop.window;

import com.zaxxer.hikari.HikariDataSource;
import org.hibernate.SessionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Watches, from a thread of its own, the three moments that decide how a late commit ends:
 * <ol>
 *     <li>Hibernate is closed: from now on it skips writing out its in-memory changes at commit;</li>
 *     <li>HikariCP starts shutting down (the moment it logs "Shutdown initiated");</li>
 *     <li>HikariCP closes the transaction's connection: from now on the commit can only fail.</li>
 * </ol>
 * It spins instead of sleeping, so it sees each of them within a microsecond or so. It only reads flags that Hibernate,
 * HikariCP and the Postgres driver keep anyway, so it changes nothing in how the shutdown goes.
 */
final class ShutdownStopwatch {

    static final long NOT_SEEN = Long.MIN_VALUE;

    private final SessionFactory hibernate;
    private final HikariDataSource pool;
    private final Connection connection;
    private final Thread thread;
    private volatile long hibernateClosed = NOT_SEEN;
    private volatile long poolShutdown = NOT_SEEN;
    private volatile long connectionClosed = NOT_SEEN;

    private ShutdownStopwatch(SessionFactory hibernate, HikariDataSource pool, Connection connection) {
        this.hibernate = hibernate;
        this.pool = pool;
        this.connection = connection;
        this.thread = new Thread(this::watch, "shutdown-stopwatch");
        this.thread.setDaemon(true);
    }

    static ShutdownStopwatch start(SessionFactory hibernate, HikariDataSource pool, Connection connection) {
        ShutdownStopwatch stopwatch = new ShutdownStopwatch(hibernate, pool, connection);
        stopwatch.thread.start();
        return stopwatch;
    }

    private void watch() {
        long giveUp = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
        while ((hibernateClosed == NOT_SEEN || poolShutdown == NOT_SEEN || connectionClosed == NOT_SEEN)
                && System.nanoTime() < giveUp) {
            if (hibernateClosed == NOT_SEEN && !hibernate.isOpen()) {
                hibernateClosed = System.nanoTime();
            }
            if (poolShutdown == NOT_SEEN && pool.isClosed()) {
                poolShutdown = System.nanoTime();
            }
            if (connectionClosed == NOT_SEEN && isClosed(connection)) {
                connectionClosed = System.nanoTime();
            }
            Thread.onSpinWait();
        }
    }

    /**
     * The driver's own flag: true once the socket to Postgres has been closed, by an abort or by a regular close.
     */
    private static boolean isClosed(Connection connection) {
        try {
            return connection.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    void awaitAllSeen() throws InterruptedException {
        thread.join(TimeUnit.MINUTES.toMillis(2));
    }

    long hibernateClosed() {
        return hibernateClosed;
    }

    long poolShutdown() {
        return poolShutdown;
    }

    long connectionClosed() {
        return connectionClosed;
    }
}
