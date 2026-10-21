package shop.window;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.SessionFactory;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import shop.ShopApplication;
import shop.payment.BackgroundPayments;
import shop.payment.PaymentService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Runs in a JVM of its own. For each moment of its plan it starts a shop pod, records one payment in an @Async task,
 * stops the pod while the task is in the middle of its transaction, lets the task commit at that moment, and prints
 * one {@link WindowRun} line.
 * <p>
 * A fresh JVM matters: a real pod shuts down once, so Spring's, Hibernate's and HikariCP's shutdown code runs cold.
 * Several shutdowns in one JVM warm that code up, and the window shrinks.
 * <p>
 * Arguments:
 * <pre>
 * --variant=without-metrics-exporter | with-metrics-exporter
 * --plan=hibernate-closed+0,hibernate-closed+500,closing-log+0   (cue + delay in microseconds, one shutdown each)
 * --warmup=1000                     (payments recorded before the measured one, to warm up the commit path)
 * --trace-destroy                   (log every bean Spring destroys, to show what is closed in the window)
 * --db-url=... --db-user=... --db-password=...
 * --pod-arg=...                     (more arguments for the pod, e.g. a broker profile; may repeat)
 * </pre>
 */
public final class ShutdownWindowProbe {

    static final String WITHOUT_METRICS_EXPORTER = "without-metrics-exporter";
    static final String WITH_METRICS_EXPORTER = "with-metrics-exporter";

    /**
     * Commit this long after Hibernate is closed.
     */
    static final String HIBERNATE_CLOSED = "hibernate-closed";

    /**
     * Commit this long after Spring logs "Closing JPA EntityManagerFactory", i.e. while Hibernate is still closing.
     */
    static final String CLOSING_LOG = "closing-log";

    /**
     * How long Spring waits for the @Async task before it gives up. It only decides when Spring stops waiting, not
     * what happens after, so the default 30 s would only make each run 30 s longer.
     */
    private static final String SPRING_WAITS = "500ms";

    private record Settings(String variant, List<String> plan, int warmup, boolean traceDestroy,
                            String dbUrl, String dbUser, String dbPassword, List<String> podArguments) {

        static Settings parse(String[] args) {
            String variant = WITHOUT_METRICS_EXPORTER;
            List<String> plan = List.of(HIBERNATE_CLOSED + "+0");
            int warmup = 1000;
            boolean traceDestroy = false;
            String dbUrl = null;
            String dbUser = null;
            String dbPassword = null;
            List<String> podArguments = new ArrayList<>();
            for (String arg : args) {
                String value = arg.substring(arg.indexOf('=') + 1);
                if (arg.startsWith("--variant=")) {
                    variant = value;
                } else if (arg.startsWith("--plan=")) {
                    plan = List.of(value.split(","));
                } else if (arg.startsWith("--warmup=")) {
                    warmup = Integer.parseInt(value);
                } else if (arg.equals("--trace-destroy")) {
                    traceDestroy = true;
                } else if (arg.startsWith("--db-url=")) {
                    dbUrl = value;
                } else if (arg.startsWith("--db-user=")) {
                    dbUser = value;
                } else if (arg.startsWith("--db-password=")) {
                    dbPassword = value;
                } else if (arg.startsWith("--pod-arg=")) {
                    podArguments.add(value);
                } else {
                    throw new IllegalArgumentException("unknown argument " + arg);
                }
            }
            if (!variant.equals(WITHOUT_METRICS_EXPORTER) && !variant.equals(WITH_METRICS_EXPORTER)) {
                throw new IllegalArgumentException("unknown variant " + variant);
            }
            return new Settings(variant, plan, warmup, traceDestroy, dbUrl, dbUser, dbPassword, podArguments);
        }

        boolean withMetricsExporter() {
            return variant.equals(WITH_METRICS_EXPORTER);
        }
    }

    private ShutdownWindowProbe() {
    }

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.parse(args);
        int run = 0;
        for (String moment : settings.plan()) {
            WindowRun result = shutdownOnce(settings, moment, ++run);
            System.out.println(result.toLine());
            System.out.flush();
        }
        System.exit(0);
    }

    private static WindowRun shutdownOnce(Settings settings, String moment, int runInJvm) throws Exception {
        String cueName = moment.substring(0, moment.indexOf('+'));
        long delayMicros = Long.parseLong(moment.substring(moment.indexOf('+') + 1));

        ConfigurableApplicationContext pod = startPod(settings);
        SingleConnectionDataSource databaseConnection = new SingleConnectionDataSource(settings.dbUrl(), settings.dbUser(), settings.dbPassword(), true);
        JdbcTemplate database = new JdbcTemplate(databaseConnection);
        SessionFactory hibernate = pod.getBean(EntityManagerFactory.class).unwrap(SessionFactory.class);
        HikariDataSource pool = pod.getBean(DataSource.class).unwrap(HikariDataSource.class);
        TimedFraudCheck fraudCheck = pod.getBean(TimedFraudCheck.class);
        CommitClock commit = pod.getBean(CommitClock.class);

        warmUp(pod.getBean(PaymentService.class), database, settings.warmup());

        String orderId = "order-" + UUID.randomUUID().toString().substring(0, 8);
        database.update("insert into orders (id, status) values (?, 'NEW')", orderId);

        ShutdownLog log = ShutdownLog.attach();
        if (settings.traceDestroy()) {
            log.traceBeanDestruction();
        }
        BooleanSupplier cue = switch (cueName) {
            case HIBERNATE_CLOSED -> fraudCheck.hibernateClosed();
            case CLOSING_LOG -> log::closingHibernateLogged;
            default -> throw new IllegalArgumentException("unknown cue " + cueName);
        };
        fraudCheck.answerNextPaymentAfter(cue, TimeUnit.MICROSECONDS.toNanos(delayMicros));
        commit.watchNextTransaction();
        pod.getBean(BackgroundPayments.class).recordPayment(orderId);
        Connection connection = fraudCheck.awaitRequest();

        ShutdownStopwatch stopwatch = ShutdownStopwatch.start(hibernate, pool, connection);
        Thread sigterm = new Thread(pod::close, "sigterm");
        sigterm.start();
        commit.awaitEnd();
        sigterm.join(TimeUnit.MINUTES.toMillis(2));
        fraudCheck.paymentThread().join(TimeUnit.MINUTES.toMillis(1));
        stopwatch.awaitAllSeen();
        log.detach();

        long t0 = stopwatch.hibernateClosed();
        int payments = database.queryForObject("select count(*) from payments where order_id = ?", Integer.class, orderId);
        String status = database.queryForObject("select status from orders where id = ?", String.class, orderId);
        databaseConnection.destroy();
        return new WindowRun(settings.variant(), runInJvm, moment,
                micros(log.whenLogged(ShutdownLog.CLOSING_HIBERNATE), t0),
                micros(stopwatch.poolShutdown(), t0),
                micros(log.whenLogged(ShutdownLog.POOL_SHUTDOWN), t0),
                micros(stopwatch.connectionClosed(), t0),
                micros(fraudCheck.answeredAt(), t0),
                micros(commit.started(), t0),
                micros(commit.ended(), t0),
                payments, status, commit.ending(),
                applicationError(log, commit),
                settings.traceDestroy() ? destroySequence(log, t0) : "-");
    }

    /**
     * Starts a pod: the shop application through its own main, configured through its arguments like a production pod.
     */
    private static ConfigurableApplicationContext startPod(Settings settings) {
        List<String> args = new ArrayList<>(settings.podArguments());
        args.add("--server.port=0");
        args.add("--spring.datasource.url=" + settings.dbUrl());
        args.add("--spring.datasource.username=" + settings.dbUser());
        args.add("--spring.datasource.password=" + settings.dbPassword());
        args.add("--spring.lifecycle.timeout-per-shutdown-phase=" + SPRING_WAITS);

        SpringApplication.Augmented shop = SpringApplication.from(ShopApplication::main)
                .with(TimedFraudCheck.class, CommitClock.class);
        if (!settings.withMetricsExporter()) {
            shop = shop.with(WithoutMetricsExporter.class);
        }
        ConfigurableApplicationContext pod = shop.run(args.toArray(String[]::new)).getApplicationContext();
        if (pod.containsBean("metricsExporter") != settings.withMetricsExporter()) {
            throw new IllegalStateException("the pod should " + (settings.withMetricsExporter() ? "" : "not ") + "have MetricsExporter");
        }
        return pod;
    }

    /**
     * Records payments through the same code first, so that the commit path, the flush and its UPDATE included, is as
     * warm as in a pod that has been serving traffic for a while. The shutdown code stays cold: it has not run yet.
     */
    private static void warmUp(PaymentService payments, JdbcTemplate database, int transactions) {
        String orderId = "warm-up-" + UUID.randomUUID().toString().substring(0, 8);
        database.update("insert into orders (id, status) values (?, 'NEW')", orderId);
        Logger paymentLog = (Logger) LoggerFactory.getLogger(PaymentService.class);
        Level level = paymentLog.getLevel();
        paymentLog.setLevel(Level.WARN);
        try {
            for (int i = 0; i < transactions; i++) {
                // back to NEW, so that every warm-up payment really changes the order and Hibernate sends an UPDATE
                database.update("update orders set status = 'NEW' where id = ?", orderId);
                payments.recordPayment(orderId);
            }
        } finally {
            paymentLog.setLevel(level);
        }
    }

    /**
     * What the application got: the error the @Async task ended with, as Spring logged it, or the commit's own error.
     */
    private static String applicationError(ShutdownLog log, CommitClock commit) {
        IThrowableProxy error = log.asyncTaskError();
        if (error == null && commit.failure() != null) {
            error = new ThrowableProxy(commit.failure());
        }
        if (error == null) {
            return "-";
        }
        StringJoiner chain = new StringJoiner(" <- ");
        IThrowableProxy deepest = error;
        for (IThrowableProxy e = error; e != null; e = e.getCause()) {
            chain.add(e.getClassName().substring(e.getClassName().lastIndexOf('.') + 1));
            deepest = e;
        }
        return chain + ": " + deepest.getMessage();
    }

    private static String destroySequence(ShutdownLog log, long t0) {
        StringJoiner sequence = new StringJoiner(" → ");
        for (ShutdownLog.DestroyedBean bean : log.destructionFromHibernateToThePool()) {
            sequence.add(bean.name() + " " + signed(micros(bean.nanos(), t0)));
        }
        return sequence.length() == 0 ? "-" : sequence.toString();
    }

    private static long micros(long nanos, long t0) {
        if (nanos == ShutdownStopwatch.NOT_SEEN || t0 == ShutdownStopwatch.NOT_SEEN) {
            return ShutdownStopwatch.NOT_SEEN;
        }
        return Math.floorDiv(nanos - t0, 1000);
    }

    private static String signed(long micros) {
        return (micros >= 0 ? "+" : "") + micros;
    }
}
