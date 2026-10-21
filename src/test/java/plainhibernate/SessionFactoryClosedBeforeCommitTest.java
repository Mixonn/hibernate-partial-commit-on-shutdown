package plainhibernate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.transaction.Synchronization;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.resource.transaction.LocalSynchronizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Plain Hibernate, no Spring: the SessionFactory is closed while another thread has an open transaction, and that
 * thread's commit() then saves only part of the transaction, without an exception. The same happens when close() and
 * commit() run on one thread; without close() both changes are committed.
 */
class SessionFactoryClosedBeforeCommitTest {

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @Entity
    @Table(name = "orders")
    static class Order {
        @Id
        Long id;
        String status;
    }

    @Entity
    @Table(name = "payments")
    static class Payment {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY) // the INSERT is executed at persist(), not at flush
        Long id;
        Long orderId;
    }

    SessionFactory sessionFactory;

    @BeforeEach
    void setUp() throws SQLException {
        // The connection pool belongs to the application, as in Spring, Quarkus or an application server.
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());

        Configuration configuration = new Configuration()
                .addAnnotatedClass(Order.class)
                .addAnnotatedClass(Payment.class)
                .setProperty(AvailableSettings.HBM2DDL_AUTO, "create");
        configuration.getProperties().put(AvailableSettings.DATASOURCE, dataSource);
        sessionFactory = configuration.buildSessionFactory();

        sql("insert into orders (id, status) values (1, 'NEW')");
    }

    @AfterEach
    void tearDown() {
        if (!sessionFactory.isClosed()) {
            sessionFactory.close();
        }
    }

    @Test
    void withoutCloseBothChangesAreCommitted() throws Exception {
        try (Session session = sessionFactory.openSession()) {
            Transaction transaction = session.beginTransaction();
            Payment payment = new Payment();
            payment.orderId = 1L;
            session.persist(payment);
            session.find(Order.class, 1L).status = "PAID";
            transaction.commit();
        }

        assertEquals(1, count("select count(*) from payments where orderId = 1"));
        assertEquals("PAID", status());
    }

    @Test
    void closeAndCommitOnTheSameThreadAlsoSaveOnlyPartOfTheTransaction() throws Exception {
        Session session = sessionFactory.openSession();
        Transaction transaction = session.beginTransaction();
        Payment payment = new Payment();
        payment.orderId = 1L;
        session.persist(payment);
        session.find(Order.class, 1L).status = "PAID";

        sessionFactory.close();
        transaction.commit();
        session.close();

        assertEquals(1, count("select count(*) from payments where orderId = 1"), "the INSERT was committed");
        assertEquals("NEW", status(), "the UPDATE was never executed, yet the transaction was committed");
    }

    @Test
    void commitAfterSessionFactoryCloseSavesOnlyPartOfTheTransaction() throws Exception {
        Throwable commitFailure = commitAfterSessionFactoryIsClosed(null);

        assertNull(commitFailure, "commit() reported success");
        assertEquals(1, count("select count(*) from payments where orderId = 1"), "the INSERT was committed");
        assertEquals("NEW", status(), "the UPDATE was never executed, yet the transaction was committed");
    }

    @Test
    void guardSynchronizationTurnsThePartialCommitIntoARollback() throws Exception {
        Synchronization guard = new Synchronization() {
            @Override
            public void beforeCompletion() {
                // Runs after Session.beforeTransactionCompletion(), i.e. after the flush was (or was not) done.
                if (sessionFactory.isClosed()) {
                    throw new IllegalStateException("SessionFactory was closed before commit: rolling back");
                }
            }

            @Override
            public void afterCompletion(int status) {
            }
        };

        Throwable commitFailure = commitAfterSessionFactoryIsClosed(guard);

        assertInstanceOf(LocalSynchronizationException.class, commitFailure, "commit() failed because of the guard");
        assertEquals("SessionFactory was closed before commit: rolling back", commitFailure.getCause().getMessage());
        assertEquals(0, count("select count(*) from payments where orderId = 1"), "the INSERT was rolled back");
        assertEquals("NEW", status());
    }

    /**
     * Worker thread: begin, INSERT a payment, change the order, then wait. Main thread: close the SessionFactory.
     * Worker thread: commit. Returns what commit() threw, or null.
     */
    private Throwable commitAfterSessionFactoryIsClosed(Synchronization synchronization) throws Exception {
        CountDownLatch changesMade = new CountDownLatch(1);
        CountDownLatch factoryClosed = new CountDownLatch(1);

        CompletableFuture<Throwable> worker = CompletableFuture.supplyAsync(() -> {
            Session session = sessionFactory.openSession();
            Transaction transaction = session.beginTransaction();
            if (synchronization != null) {
                transaction.registerSynchronization(synchronization);
            }

            Payment payment = new Payment();
            payment.orderId = 1L;
            session.persist(payment);                     // INSERT is sent to the database now
            session.find(Order.class, 1L).status = "PAID"; // UPDATE waits for the flush at commit

            changesMade.countDown();
            await(factoryClosed);
            try {
                transaction.commit();
                return null;
            } catch (RuntimeException e) {
                return e;
            } finally {
                session.close();
            }
        });

        await(changesMade);
        sessionFactory.close();
        factoryClosed.countDown();
        return worker.get(10, TimeUnit.SECONDS);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out");
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    // A separate JDBC connection, outside Hibernate.

    private static String status() throws SQLException {
        try (Connection c = connect(); ResultSet rs = c.createStatement().executeQuery("select status from orders where id = 1")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static int count(String query) throws SQLException {
        try (Connection c = connect(); ResultSet rs = c.createStatement().executeQuery(query)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static void sql(String statement) throws SQLException {
        try (Connection c = connect()) {
            c.createStatement().execute(statement);
        }
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }
}
