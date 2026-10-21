package shop.window;

import java.util.Arrays;
import java.util.List;

/**
 * One measured shutdown. All times are in microseconds from the moment Hibernate was closed (negative: before it).
 * The probe prints it as one tab-separated line; the measurement reads it back.
 *
 * @param variant            which shop: {@code without-metrics-exporter} or {@code with-metrics-exporter}
 * @param runInJvm           1 for the first shutdown in its JVM, i.e. with Spring's, Hibernate's and HikariCP's
 *                           shutdown code still cold, like in a real pod
 * @param moment             when the fraud check was told to answer, e.g. {@code hibernate-closed+100}
 * @param closingLogUs       Spring logged "Closing JPA EntityManagerFactory"
 * @param poolShutdownUs     HikariCP started shutting down
 * @param poolLogUs          HikariCP logged "Shutdown initiated"
 * @param connectionClosedUs HikariCP closed the transaction's connection: the end of the window
 * @param answeredUs         the fraud check answered, the payment service goes on to commit
 * @param commitStartUs      Spring started the commit
 * @param commitEndUs        the commit (or the rollback after a failed commit) was over
 * @param payments           payments saved for the order, read from the database afterwards
 * @param orderStatus        the order's status, read from the database afterwards
 * @param ending             how Spring ended the transaction: COMMIT or ROLLBACK
 * @param appError           the error the application got, or "-"
 * @param destroySequence    the beans Spring destroyed from Hibernate to the pool, with the microsecond each one
 *                           started, e.g. {@code entityManagerFactory -350 → dataSource +470} (only with bean tracing
 *                           on), or "-"
 */
record WindowRun(String variant, int runInJvm, String moment,
                 long closingLogUs, long poolShutdownUs, long poolLogUs, long connectionClosedUs,
                 long answeredUs, long commitStartUs, long commitEndUs,
                 int payments, String orderStatus, String ending, String appError, String destroySequence) {

    static final String MARKER = "WINDOW-RESULT";

    enum Outcome {
        FULL("full commit"),
        FULL_WITH_ERROR("full commit, the application got an error"),
        PARTIAL("partial commit, no error"),
        PARTIAL_WITH_ERROR("partial commit, the application got an error"),
        NOTHING_WITH_ERROR("nothing saved, the application got an error"),
        OTHER("something else");

        final String description;

        Outcome(String description) {
            this.description = description;
        }
    }

    Outcome outcome() {
        boolean applicationSawSuccess = ending.equals("COMMIT") && appError.equals("-");
        if (payments == 1 && orderStatus.equals("PAID")) {
            return applicationSawSuccess ? Outcome.FULL : Outcome.FULL_WITH_ERROR;
        }
        if (payments == 1 && orderStatus.equals("NEW")) {
            return applicationSawSuccess ? Outcome.PARTIAL : Outcome.PARTIAL_WITH_ERROR;
        }
        if (payments == 0 && orderStatus.equals("NEW") && !applicationSawSuccess) {
            return Outcome.NOTHING_WITH_ERROR;
        }
        return Outcome.OTHER;
    }

    String toLine() {
        return String.join("\t", MARKER, variant, String.valueOf(runInJvm), moment,
                us(closingLogUs), us(poolShutdownUs), us(poolLogUs), us(connectionClosedUs),
                us(answeredUs), us(commitStartUs), us(commitEndUs),
                String.valueOf(payments), orderStatus, ending, clean(appError), clean(destroySequence));
    }

    static boolean isLine(String line) {
        return line.startsWith(MARKER + "\t");
    }

    static WindowRun parse(String line) {
        List<String> f = Arrays.asList(line.split("\t", -1));
        return new WindowRun(f.get(1), Integer.parseInt(f.get(2)), f.get(3),
                us(f.get(4)), us(f.get(5)), us(f.get(6)), us(f.get(7)),
                us(f.get(8)), us(f.get(9)), us(f.get(10)),
                Integer.parseInt(f.get(11)), f.get(12), f.get(13), f.get(14), f.get(15));
    }

    private static String us(long micros) {
        return micros == ShutdownStopwatch.NOT_SEEN ? "-" : String.valueOf(micros);
    }

    private static long us(String text) {
        return text.equals("-") ? ShutdownStopwatch.NOT_SEEN : Long.parseLong(text);
    }

    private static String clean(String text) {
        return text.replaceAll("\\s+", " ").trim();
    }
}
