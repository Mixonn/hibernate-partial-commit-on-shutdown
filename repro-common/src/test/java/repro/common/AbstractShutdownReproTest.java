package repro.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Prawdziwa aplikacja Spring Boot, prawdziwy broker i Postgres. Test wysyła wiadomość, czeka, aż handler
 * będzie w środku transakcji, zamyka kontekst tak jak shutdown hook po SIGTERM i sprawdza, co zostało w bazie.
 */
public abstract class AbstractShutdownReproTest {

    /** log_statement=all: serwer loguje każde zapytanie, które naprawdę do niego dotarło. */
    protected static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all", "-c", "log_line_prefix=%m [%p] ");

    static {
        POSTGRES.start();
    }

    public enum Outcome { FULL, NONE, PARTIAL, OTHER }

    public record Result(Outcome outcome, int rows, int counter, String failure, List<String> causeChain, boolean interrupted,
                         long emfClosedSeenMs, long tailExitMs, long finishedMs, long closeMs) {
    }

    protected abstract Class<?> application();

    protected abstract Map<String, Object> brokerProperties();

    protected abstract void send(ConfigurableApplicationContext context, String msg) throws Exception;

    private String destination;
    private String lastMsg;
    private long seedXmin;

    /** Osobny topic albo kolejka na każdy przebieg, żeby niepotwierdzona wiadomość nie wróciła w następnym. */
    protected String destination() {
        return destination;
    }

    /** Dodatkowe konfiguracje dokładane do kontekstu aplikacji; wariant z fixem dokłada tu konfigurację weta. */
    protected Class<?>[] extraConfiguration() {
        return new Class<?>[0];
    }

    private boolean vetoEnabled() {
        return extraConfiguration().length > 0;
    }

    /**
     * Dowód od strony Postgresa, niezależny od logów aplikacji i od poziomu izolacji: log serwera z tego
     * połączenia oraz xmin, czyli numer transakcji, która zapisała bieżącą wersję każdego wiersza.
     */
    @Test
    void evidence_serverSideViewOfPartialCommit() throws Exception {
        Result result = run(Hooks.Tail.UNTIL_EMF_CLOSED, 0, 500);
        long rowXmin = queryLong("select xmin::text::bigint from identity_row where msg = '" + lastMsg + "'");
        long counterXmin = queryLong("select xmin::text::bigint from counter where id = 1");
        List<String> session = serverLogOfTransactionThatInserted(lastMsg);
        String sessionLog = String.join("\n", session);

        System.out.println("EVIDENCE " + getClass().getSimpleName() + " outcome=" + result.outcome() + " failure=" + result.failure());
        System.out.println("EVIDENCE xmin wiersza licznika = " + counterXmin + " (transakcja, która go wstawiła na starcie testu: " + seedXmin + ")");
        System.out.println("EVIDENCE xmin nowego wiersza identity_row = " + rowXmin + " (-1 = wiersza nie ma)");
        System.out.println("EVIDENCE log serwera z połączenia handlera, od BEGIN do końca transakcji:");
        session.forEach(l -> System.out.println("EVIDENCE   " + l));

        assertThat(counterXmin).as("licznika nie zmieniła żadna zatwierdzona transakcja").isEqualTo(seedXmin);
        assertThat(sessionLog).contains("insert into identity_row").doesNotContain("update counter");
        if (vetoEnabled()) {
            assertThat(result.outcome()).isEqualTo(Outcome.NONE);
            assertThat(rowXmin).as("wiersza z INSERT-a nie ma").isEqualTo(-1);
            assertThat(sessionLog).contains("ROLLBACK").doesNotContain("COMMIT");
        } else {
            assertThat(result.outcome()).isEqualTo(Outcome.PARTIAL);
            assertThat(rowXmin).isNotEqualTo(seedXmin);
            assertThat(sessionLog).contains("COMMIT");
        }
    }

    @Test
    void control_handlerFinishesBeforeShutdownTimeout() throws Exception {
        Result result = run(Hooks.Tail.SLEEP, 500, 0);
        assertThat(result.outcome()).isEqualTo(Outcome.FULL);
    }

    /** Minimalna aplikacja: między zamknięciem EMF a zamknięciem puli Hikari mijają pojedyncze milisekundy. */
    @Test
    void minimalApp_handlerStillInTransactionWhenEmfCloses() throws Exception {
        Result result = run(Hooks.Tail.UNTIL_EMF_CLOSED, 0, 0);
        if (vetoEnabled()) {
            assertThat(result.outcome()).isEqualTo(Outcome.NONE);
        } else {
            assertThat(result.outcome()).isNotEqualTo(Outcome.FULL);
        }
    }

    /** Aplikacja z beanem, którego zamknięcie trwa 500 ms między EMF a pulą połączeń. */
    @Test
    void appWithSlowShutdownBean_handlerStillInTransactionWhenEmfCloses() throws Exception {
        Result result = run(Hooks.Tail.UNTIL_EMF_CLOSED, 0, 500);
        if (vetoEnabled()) {
            assertThat(result.outcome()).isEqualTo(Outcome.NONE);
            assertThat(result.causeChain()).as("odmowa weta jest w łańcuchu przyczyn").contains("CommitRefusedException");
        } else {
            assertThat(result.outcome()).isEqualTo(Outcome.PARTIAL);
            assertThat(result.failure()).as("commit bez żadnego wyjątku").isNull();
        }
    }

    /**
     * Handler kończy pracę w różnych momentach liczonych od startu zamykania kontekstu, bez patrzenia na EMF.
     * Pokazuje, w jakim oknie czasowym wynik jest pełny, częściowy albo cofnięty.
     */
    @Test
    void sweep_appWithSlowShutdownBean_handlerFinishTime() throws Exception {
        Result probe = run(Hooks.Tail.UNTIL_EMF_CLOSED, 0, 500);
        long emfCloseMs = probe.emfClosedSeenMs();
        List<String> lines = new ArrayList<>();
        List<Outcome> outcomes = new ArrayList<>();
        for (long offset = emfCloseMs - 200; offset <= emfCloseMs + 800; offset += 100) {
            Result r = run(Hooks.Tail.UNTIL_OFFSET_FROM_CLOSE_START, offset, 500);
            outcomes.add(r.outcome());
            lines.add(String.format("koniec pracy handlera = start zamykania +%5d ms (EMF zamknięty ok. +%d ms) -> %-7s rows=%d counter=%d wyjątek=%s",
                    offset, emfCloseMs, r.outcome(), r.rows(), r.counter(), r.failure()));
        }
        lines.forEach(l -> System.out.println("SWEEP " + getClass().getSimpleName() + " " + l));
        if (vetoEnabled()) {
            assertThat(outcomes).doesNotContain(Outcome.PARTIAL, Outcome.OTHER);
        }
    }

    protected Result run(Hooks.Tail tail, long millis, long slowDestroyMs) throws Exception {
        Hooks.reset(tail, millis);
        destination = "repro-" + UUID.randomUUID();
        String msg = "m-" + UUID.randomUUID();

        ConfigurableApplicationContext context = new SpringApplicationBuilder(application())
                .sources(extraConfiguration())
                .properties(properties(slowDestroyMs))
                .registerShutdownHook(false)
                .run();
        if (vetoEnabled()) {
            assertThat(context.getBean(AbstractPlatformTransactionManager.class).getTransactionExecutionListeners())
                    .as("Spring Boot podpiął weto do transaction managera")
                    .anyMatch(listener -> listener.getClass().getSimpleName().equals("EmfCloseVeto"));
        }
        update("insert into counter(id, value) values (1, 0)");
        seedXmin = queryLong("select xmin::text::bigint from counter where id = 1");
        lastMsg = msg;
        send(context, msg);
        assertThat(Hooks.enteredTail.await(60, TimeUnit.SECONDS)).as("handler started").isTrue();

        long closeStart = System.nanoTime();
        Hooks.closeStartNanos = closeStart;
        Thread closer = new Thread(context::close, "test-context-closer");
        closer.start();
        closer.join(TimeUnit.MINUTES.toMillis(4));
        long closeEnd = System.nanoTime();
        Hooks.finished.await(60, TimeUnit.SECONDS);

        int rows = count("select count(*) from identity_row where msg = '" + msg + "'");
        int counter = count("select value from counter where id = 1");
        Outcome outcome = rows == 1 && counter == 1 ? Outcome.FULL
                : rows == 0 && counter == 0 ? Outcome.NONE
                : rows == 1 && counter == 0 ? Outcome.PARTIAL
                : Outcome.OTHER;
        Throwable failure = Hooks.failure;
        Result result = new Result(outcome, rows, counter,
                failure == null ? null : failure.getClass().getSimpleName() + ": " + failure.getMessage(),
                causeChain(failure),
                Hooks.interruptedDuringTail,
                relativeMs(Hooks.emfClosedSeenNanos, closeStart), relativeMs(Hooks.tailExitNanos, closeStart),
                relativeMs(Hooks.finishedNanos, closeStart), relativeMs(closeEnd, closeStart));
        System.out.println("RESULT " + getClass().getSimpleName() + " veto=" + vetoEnabled() + " tail=" + tail + "(" + millis + ") slowDestroyMs=" + slowDestroyMs + " -> " + result);
        return result;
    }

    private Map<String, Object> properties(long slowDestroyMs) {
        Map<String, Object> props = new HashMap<>(brokerProperties());
        props.put("repro.destination", destination);
        if (slowDestroyMs > 0) {
            props.put("repro.slow-destroy-ms", slowDestroyMs);
        }
        props.put("spring.datasource.url", POSTGRES.getJdbcUrl());
        props.put("spring.datasource.username", POSTGRES.getUsername());
        props.put("spring.datasource.password", POSTGRES.getPassword());
        return props;
    }

    private static List<String> causeChain(Throwable failure) {
        List<String> chain = new ArrayList<>();
        for (Throwable t = failure; t != null && chain.size() < 10; t = t.getCause()) {
            chain.add(t.getClass().getSimpleName());
        }
        return chain;
    }

    private static long relativeMs(long nanos, long base) {
        return nanos == 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(nanos - base);
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static void update(String sql) throws SQLException {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.executeUpdate();
        }
    }

    /**
     * Znajduje w logu serwera połączenie, które wykonało INSERT z tą wiadomością, i zwraca jego zapytania
     * od ostatniego BEGIN przed INSERT-em do najbliższego COMMIT albo ROLLBACK.
     */
    private static List<String> serverLogOfTransactionThatInserted(String msg) {
        List<String> lines = List.of(POSTGRES.getLogs().split("\\R"));
        int detail = -1;
        for (int i = 0; i < lines.size(); i++) {
            // parametry INSERT-a handlera; zapytania kontrolne testu mają wiadomość wpisaną wprost, bez parametrów
            if (lines.get(i).contains("parameters:") && lines.get(i).contains("'" + msg + "'")) {
                detail = i;
            }
        }
        if (detail < 0) {
            return List.of("(nie znaleziono w logu serwera zapytania z " + msg + ")");
        }
        String pid = lines.get(detail).replaceAll("^.*? \\[(\\d+)\\] .*$", "$1");
        String tag = "[" + pid + "]";
        int begin = detail;
        while (begin > 0 && !(lines.get(begin).contains(tag) && lines.get(begin).contains("BEGIN"))) {
            begin--;
        }
        List<String> session = new ArrayList<>();
        for (int i = begin; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!line.contains(tag)) {
                continue;
            }
            session.add(line.replaceAll("^\\S+ \\S+ \\S+ ", ""));
            if (i > detail && (line.contains("COMMIT") || line.contains("ROLLBACK"))) {
                break;
            }
        }
        return session;
    }

    private static long queryLong(String sql) throws SQLException {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : -1;
        }
    }

    private static int count(String sql) throws SQLException {
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }
}
