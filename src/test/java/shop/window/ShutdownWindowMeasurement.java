package shop.window;

import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import shop.window.WindowRun.Outcome;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static shop.window.ShutdownWindowProbe.CLOSING_LOG;
import static shop.window.ShutdownWindowProbe.HIBERNATE_CLOSED;
import static shop.window.ShutdownWindowProbe.WITHOUT_METRICS_EXPORTER;
import static shop.window.ShutdownWindowProbe.WITH_METRICS_EXPORTER;

/**
 * How wide is the window in which a late commit saves only part of a transaction?
 * <p>
 * The window opens when Hibernate is closed (from then on it skips writing out its in-memory changes) and closes when
 * HikariCP closes the transaction's connection (from then on the commit can only fail). Every shutdown runs in a fresh
 * JVM ({@link ShutdownWindowProbe}), because a real pod shuts down once, with its shutdown code still cold. Each test
 * prints its tables and writes them, with every single run, to {@code target/window-measurement/}.
 * <p>
 * It is not part of the regular test run: it takes about a quarter of an hour. Run it with
 * {@code mvn -B test -Dtest=ShutdownWindowMeasurement}; {@code -Dwindow.repeats=N} sets how many shutdowns each
 * moment gets (default 10).
 */
class ShutdownWindowMeasurement {

    private static final int REPEATS = Integer.getInteger("window.repeats", 10);
    private static final Path RESULTS = Path.of("target", "window-measurement");

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off");

    static {
        POSTGRES.start();
    }

    /**
     * The main measurement: the shop with nothing of its own between Hibernate and the pool. The required delays, plus
     * finer ones below a millisecond, where the window turns out to be.
     */
    @Test
    void shopWithoutMetricsExporter() throws Exception {
        List<String> moments = List.of(CLOSING_LOG + "+0",
                at(0), at(100), at(200), at(300), at(400), at(500), at(700), at(1_000), at(1_500),
                at(2_000), at(5_000), at(10_000), at(20_000), at(50_000), at(100_000));

        List<WindowRun> runs = freshJvmPerShutdown("without-exporter", WITHOUT_METRICS_EXPORTER, moments, REPEATS, List.of());

        String title = "The shop without MetricsExporter, a fresh JVM for every shutdown";
        write("without-metrics-exporter", windowTable(title, runs) + "\n" + sweepTable(title, runs) + effectiveWidths(runs), runs);
        assertThat(runs).hasSize(moments.size() * REPEATS);
    }

    /**
     * The same shop, many shutdowns in one JVM: the shutdown code warms up and the window shrinks. Only to show why
     * the fresh-JVM numbers are the ones that count.
     */
    @Test
    void shopWithoutMetricsExporter_manyShutdownsInOneJvm() throws Exception {
        List<String> round = List.of(at(0), at(100), at(200), at(300), at(500), at(700), at(1_000), at(2_000));
        List<String> plan = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            plan.addAll(round);
        }

        List<WindowRun> all = probe("one-jvm", WITHOUT_METRICS_EXPORTER, plan, false, List.of());
        List<WindowRun> warm = all.stream().filter(run -> run.runInJvm() > 1).toList();

        report("without-metrics-exporter-one-jvm", "The shop without MetricsExporter, " + plan.size()
                + " shutdowns in one JVM (the first one left out)", warm);
        assertThat(all).hasSize(plan.size());
    }

    /**
     * For comparison: the demo shop, whose MetricsExporter takes 0.5 s to close between Hibernate and the pool.
     */
    @Test
    void shopWithMetricsExporter() throws Exception {
        List<String> moments = List.of(CLOSING_LOG + "+0",
                at(0), at(1_000), at(10_000), at(100_000), at(250_000), at(450_000),
                at(499_000), at(500_000), at(501_000), at(502_000), at(505_000), at(550_000));
        int repeats = Math.max(1, REPEATS / 3);

        List<WindowRun> runs = freshJvmPerShutdown("with-exporter", WITH_METRICS_EXPORTER, moments, repeats, List.of());

        report("with-metrics-exporter", "The demo shop with MetricsExporter (0.5 s), a fresh JVM for every shutdown", runs);
        assertThat(runs).hasSize(moments.size() * repeats);
    }

    /**
     * What Spring destroys between Hibernate and the pool, in the shop without a broker, with Kafka, with RabbitMQ, and
     * in the demo shop. With a broker the shop has its listener containers, producer and connection factories; the
     * window is measured there too, on runs without the extra logging.
     */
    @Test
    void whatSpringClosesBetweenHibernateAndThePool() throws Exception {
        try (KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");
             RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:4.1-management")) {
            kafka.start();
            rabbit.start();
            Map<String, List<String>> brokers = new LinkedHashMap<>();
            brokers.put("no broker", List.of());
            brokers.put("Kafka", List.of("--spring.profiles.active=kafka",
                    "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers()));
            brokers.put("RabbitMQ", List.of("--spring.profiles.active=rabbitmq",
                    "--spring.rabbitmq.addresses=amqp://%s:%s@%s:%d".formatted(rabbit.getAdminUsername(),
                            rabbit.getAdminPassword(), rabbit.getHost(), rabbit.getAmqpPort())));

            StringBuilder text = new StringBuilder("## What Spring destroys between Hibernate and the pool\n\n")
                    .append("| Shop | Broker | What Spring destroys, from Hibernate to the pool (µs after Hibernate closed) |\n")
                    .append("|---|---|---|\n");
            List<WindowRun> windows = new ArrayList<>();
            for (Map.Entry<String, List<String>> broker : brokers.entrySet()) {
                String name = broker.getKey().replace(' ', '-').toLowerCase();
                WindowRun traced = probe("traced-" + name, WITHOUT_METRICS_EXPORTER, List.of(at(100_000)), true, broker.getValue()).get(0);
                text.append("| without MetricsExporter | ").append(broker.getKey()).append(" | ").append(traced.destroySequence()).append(" |\n");
                if (!broker.getValue().isEmpty()) {
                    windows.addAll(freshJvmPerShutdown("broker-" + name, WITHOUT_METRICS_EXPORTER, List.of(at(100_000)), 3, broker.getValue()));
                }
            }
            WindowRun demo = probe("traced-demo", WITH_METRICS_EXPORTER, List.of(at(100_000)), true, List.of()).get(0);
            text.append("| with MetricsExporter | no broker | ").append(demo.destroySequence()).append(" |\n\n");
            text.append(windowTable("The shop without MetricsExporter, with Kafka and with RabbitMQ (3 runs each)", windows));

            write("what-is-between", text.toString(), windows);
            assertThat(windows).hasSize(6);
        }
    }

    // ------------------------------------------------------------------------------------------------------------

    private static String at(long micros) {
        return HIBERNATE_CLOSED + "+" + micros;
    }

    /**
     * Each moment gets {@code repeats} shutdowns, each in a fresh JVM. The moments take turns, so that whatever drifts
     * on the machine over time affects all of them alike.
     */
    private static List<WindowRun> freshJvmPerShutdown(String name, String variant, List<String> moments, int repeats,
                                                       List<String> podArguments) throws Exception {
        List<WindowRun> runs = new ArrayList<>();
        int n = 0;
        for (int repeat = 0; repeat < repeats; repeat++) {
            for (String moment : moments) {
                runs.addAll(probe(name + "-" + ++n, variant, List.of(moment), false, podArguments));
            }
        }
        return runs;
    }

    /**
     * Runs {@link ShutdownWindowProbe} in a new JVM and reads back its results. Its whole console output, with the
     * millisecond timestamps of every log line, stays in {@code target/window-measurement/logs/}.
     */
    private static List<WindowRun> probe(String name, String variant, List<String> plan, boolean traceDestroy,
                                         List<String> podArguments) throws Exception {
        Path log = RESULTS.resolve("logs").resolve(name + ".log");
        Files.createDirectories(log.getParent());
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                ShutdownWindowProbe.class.getName(),
                "--variant=" + variant,
                "--plan=" + String.join(",", plan),
                "--db-url=" + POSTGRES.getJdbcUrl(),
                "--db-user=" + POSTGRES.getUsername(),
                "--db-password=" + POSTGRES.getPassword()));
        if (traceDestroy) {
            command.add("--trace-destroy");
        }
        podArguments.forEach(arg -> command.add("--pod-arg=" + arg));

        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!process.waitFor(10, TimeUnit.MINUTES)) {
            process.destroyForcibly();
            throw new IllegalStateException("the probe did not finish, see " + log);
        }
        List<WindowRun> runs = Files.readAllLines(log).stream().filter(WindowRun::isLine).map(WindowRun::parse).toList();
        if (runs.size() != plan.size()) {
            throw new IllegalStateException("the probe printed " + runs.size() + " of " + plan.size() + " results, see " + log);
        }
        return runs;
    }

    // ------------------------------------------------------------------------------------------------------------

    private static void report(String name, String title, List<WindowRun> runs) throws IOException {
        String text = windowTable(title, runs) + "\n" + sweepTable(title, runs);
        write(name, text, runs);
    }

    /**
     * The sweep folded into one number per outcome: the share of runs with that outcome at each delay, integrated over
     * the delays (trapezoids). It is as good as the grid: it needs rows close enough together where the share changes,
     * and a last row where it has dropped to zero.
     */
    private static String effectiveWidths(List<WindowRun> runs) {
        Map<Long, List<WindowRun>> byDelay = new TreeMap<>();
        runs.stream().filter(run -> run.moment().startsWith(HIBERNATE_CLOSED))
                .forEach(run -> byDelay.computeIfAbsent(Long.parseLong(run.moment().substring(run.moment().indexOf('+') + 1)),
                        d -> new ArrayList<>()).add(run));
        List<Long> delays = new ArrayList<>(byDelay.keySet());
        double silent = 0;
        double partial = 0;
        for (int i = 0; i + 1 < delays.size(); i++) {
            long width = delays.get(i + 1) - delays.get(i);
            silent += width * (share(byDelay.get(delays.get(i)), true) + share(byDelay.get(delays.get(i + 1)), true)) / 2;
            partial += width * (share(byDelay.get(delays.get(i)), false) + share(byDelay.get(delays.get(i + 1)), false)) / 2;
        }
        return "\nEffective width (the share of each outcome integrated over the delays): silent partial commits "
                + ms(Math.round(silent)) + ", partial commits of either kind " + ms(Math.round(partial)) + ".\n";
    }

    private static double share(List<WindowRun> runs, boolean silentOnly) {
        long count = runs.stream().filter(run -> run.outcome() == Outcome.PARTIAL
                || (!silentOnly && run.outcome() == Outcome.PARTIAL_WITH_ERROR)).count();
        return (double) count / runs.size();
    }

    private static void write(String name, String text, List<WindowRun> runs) throws IOException {
        Files.createDirectories(RESULTS);
        Files.writeString(RESULTS.resolve(name + ".md"), text);
        List<String> lines = new ArrayList<>();
        lines.add(String.join("\t", "marker", "variant", "runInJvm", "moment", "closingLogUs", "poolShutdownUs", "poolLogUs",
                "connectionClosedUs", "answeredUs", "commitStartUs", "commitEndUs", "payments", "orderStatus", "ending",
                "appError", "destroySequence", "outcome"));
        runs.forEach(run -> lines.add(run.toLine() + "\t" + run.outcome()));
        Files.write(RESULTS.resolve(name + ".tsv"), lines);
        System.out.println("\n" + text);
    }

    private static String windowTable(String title, List<WindowRun> runs) {
        // When the commit came back before the pool closed the connection, the pool closed it later as an idle one, so
        // its closing time says less about the window. These are the runs where the connection was still in use.
        List<WindowRun> stillInUse = runs.stream()
                .filter(run -> run.connectionClosedUs() != ShutdownStopwatch.NOT_SEEN && run.commitStartUs() > run.connectionClosedUs())
                .toList();
        StringBuilder text = new StringBuilder("## ").append(title).append(": the window\n\n")
                .append("| Interval | Runs | Min | p10 | Median | p90 | Max |\n|---|---|---|---|---|---|---|\n");
        row(text, "\"Closing JPA EntityManagerFactory\" logged → Hibernate closed", runs,
                run -> between(run.closingLogUs(), 0));
        row(text, "Hibernate closed → HikariCP starts shutting down", runs, WindowRun::poolShutdownUs);
        row(text, "HikariCP starts shutting down → it closes the transaction's connection (still in use)", stillInUse,
                run -> between(run.poolShutdownUs(), run.connectionClosedUs()));
        row(text, "\"Closing JPA EntityManagerFactory\" logged → \"HikariPool-1 - Shutdown initiated...\" logged", runs,
                run -> between(run.closingLogUs(), run.poolLogUs()));
        row(text, "Hibernate closed → HikariCP closes the transaction's connection, all runs", runs, WindowRun::connectionClosedUs);
        row(text, "**Hibernate closed → HikariCP closes the transaction's connection, still in use (the window)**", stillInUse,
                WindowRun::connectionClosedUs);
        return text.toString();
    }

    private static long between(long fromUs, long toUs) {
        return fromUs == ShutdownStopwatch.NOT_SEEN || toUs == ShutdownStopwatch.NOT_SEEN ? ShutdownStopwatch.NOT_SEEN : toUs - fromUs;
    }

    private static void row(StringBuilder text, String label, List<WindowRun> runs, ToLongFunction<WindowRun> micros) {
        List<Long> values = runs.stream().mapToLong(micros)
                .filter(v -> v != ShutdownStopwatch.NOT_SEEN)
                .sorted().boxed().toList();
        if (values.isEmpty()) {
            text.append("| ").append(label).append(" | 0 | | | | | |\n");
            return;
        }
        int n = values.size();
        text.append("| ").append(label).append(" | ").append(n)
                .append(" | ").append(ms(values.get(0)))
                .append(" | ").append(ms(values.get(n / 10)))
                .append(" | ").append(ms(median(values)))
                .append(" | ").append(ms(values.get(n - 1 - n / 10)))
                .append(" | ").append(ms(values.get(n - 1))).append(" |\n");
    }

    private static String sweepTable(String title, List<WindowRun> runs) {
        Map<String, List<WindowRun>> byMoment = new LinkedHashMap<>();
        runs.forEach(run -> byMoment.computeIfAbsent(run.moment(), m -> new ArrayList<>()).add(run));
        StringBuilder text = new StringBuilder("## ").append(title).append(": the sweep\n\n")
                .append("| Fraud check answers | Runs | Full commit | Full, app got an error | Partial, no error | Partial, app got an error | Nothing saved, app got an error | Other | Commit starts (median) | Commit returns (median) | Window closes (median) |\n")
                .append("|---|---|---|---|---|---|---|---|---|---|---|\n");
        for (Map.Entry<String, List<WindowRun>> entry : byMoment.entrySet()) {
            List<WindowRun> group = entry.getValue();
            text.append("| ").append(describe(entry.getKey())).append(" | ").append(group.size());
            for (Outcome outcome : Outcome.values()) {
                long count = group.stream().filter(run -> run.outcome() == outcome).count();
                text.append(" | ").append(count == 0 ? "" : count);
            }
            text.append(" | ").append(ms(median(group, WindowRun::commitStartUs)))
                    .append(" | ").append(ms(median(group, WindowRun::commitEndUs)))
                    .append(" | ").append(ms(median(group, WindowRun::connectionClosedUs))).append(" |\n");
        }
        text.append("\nTimes are from the moment Hibernate was closed. Errors the application got:\n\n");
        Map<String, Integer> errors = new LinkedHashMap<>();
        runs.stream().filter(run -> !run.appError().equals("-"))
                .forEach(run -> errors.merge(run.outcome().description + ": " + run.appError(), 1, Integer::sum));
        errors.forEach((error, count) -> text.append("- ").append(count).append("× ").append(error).append('\n'));
        return text.toString();
    }

    private static String describe(String moment) {
        long micros = Long.parseLong(moment.substring(moment.indexOf('+') + 1));
        if (moment.startsWith(CLOSING_LOG)) {
            return "when Spring logs \"Closing JPA EntityManagerFactory\"" + (micros == 0 ? "" : " + " + ms(micros));
        }
        return ms(micros) + " after Hibernate is closed";
    }

    private static long median(Collection<WindowRun> runs, ToLongFunction<WindowRun> micros) {
        List<Long> values = runs.stream().mapToLong(micros).filter(v -> v != ShutdownStopwatch.NOT_SEEN).sorted().boxed().toList();
        return values.isEmpty() ? ShutdownStopwatch.NOT_SEEN : median(values);
    }

    private static long median(List<Long> sorted) {
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2;
    }

    private static String ms(long micros) {
        if (micros == ShutdownStopwatch.NOT_SEEN) {
            return "-";
        }
        return String.format(Locale.ROOT, "%.3f ms", micros / 1000.0);
    }
}
