package shop;

import static org.assertj.core.api.Assertions.assertThat;

import fix.PartialCommitGuard;
import jakarta.persistence.EntityManagerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;
import shop.infra.ProcessingOutcomes;
import shop.infra.ProcessingOutcomes.Outcome;
import shop.payment.FraudCheckClient;

/**
 * The story: a customer pays for an order, and while that payment is being handled, Kubernetes stops the pod.
 * <p>
 * The same scenario runs on RabbitMQ and on Kafka; the test classes in shop-rabbitmq and shop-kafka only say how
 * to send the message. The steps of the story show up on the console as "STEP" lines.
 */
public abstract class PaymentShutdownStory {

    /** Postgres writes every statement that actually reached it to its own log, together with the transaction number. */
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all", "-c", "log_line_prefix=%m [%p] xid=%x ");

    static {
        POSTGRES.start();
    }

    private static final Logger story = LoggerFactory.getLogger("shop.STORY");

    private final String orderId = "order-" + UUID.randomUUID().toString().substring(0, 8);
    private final String paymentsQueue = "payments-" + UUID.randomUUID();
    private final Database database = new Database();
    private long orderCreatedInTransaction;
    private int step;

    /** How to connect the application to the broker. */
    protected abstract Map<String, Object> brokerProperties();

    /** How to send the "customer paid for the order" message. */
    protected abstract void sendPaymentReceived(ConfigurableApplicationContext shop, String orderId) throws Exception;

    /** How long Spring waits for the handler before it gives up and starts shutting down the rest of the application. */
    protected abstract String howLongSpringWaitsForTheHandler();

    protected String paymentsQueue() {
        return paymentsQueue;
    }

    @Test
    void withoutFix_podShutdownSavesHalfOfThePayment() throws Exception {
        ConfigurableApplicationContext shop = startShop(false);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(shop);

        step("According to the application, processing succeeded: " + outcome);
        step("In the database: " + database.describe(orderId));
        assertThat(outcome.succeeded()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(1);       // the payment is there...
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");   // ...but the order is still unpaid
        step("RESULT: payment saved, order unpaid. The data is inconsistent and there is no error in the logs.");
    }

    @Test
    void withFix_podShutdownSavesNothingAndTheRetrySucceeds() throws Exception {
        ConfigurableApplicationContext shop = startShop(true);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(shop);

        step("According to the application, processing failed: " + outcome);
        step("In the database: " + database.describe(orderId));
        assertThat(outcome.causedBy("HibernateClosedBeforeCommitException")).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(0);
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");

        step("The message was not acknowledged, so the broker still has it. A new pod starts");
        ConfigurableApplicationContext newPod = startShop(true);
        Outcome retry = newPod.getBean(ProcessingOutcomes.class).awaitNext();
        step("The new pod processed the same message: " + retry);
        step("In the database: " + database.describe(orderId));
        newPod.close();
        assertThat(retry.succeeded()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(1);
        assertThat(database.orderStatus(orderId)).isEqualTo("PAID");
        step("RESULT: nothing was saved halfway, and after the restart the payment was processed in full.");
    }

    /**
     * Hard evidence without the fix, straight from Postgres: what the database received from the handler and which
     * transaction last changed the order. It depends neither on the application's logs nor on the isolation level.
     */
    @Test
    void databaseProof_withoutFix_theStatusChangeNeverReachedTheDatabase() throws Exception {
        ConfigurableApplicationContext shop = startShop(false);
        paymentArrivesAndPodIsStoppedInTheMiddle(shop);

        HandlerTransaction handler = PostgresJournal.transactionThatSavedPaymentFor(orderId);
        step("Postgres log: everything it received from the handler in this transaction:" + handler.describe());
        step("Postgres about the order: it was last changed by transaction no. " + database.orderLastChangedByTransaction(orderId)
                + ", the one in which the test created the order (no. " + orderCreatedInTransaction + ")");
        step("Postgres about the payment: it was saved by transaction no. " + database.paymentSavedByTransaction(orderId)
                + ", the handler's transaction (no. " + handler.number() + ", ended with: " + handler.ending() + ")");

        assertThat(handler.statements()).anyMatch(sql -> sql.startsWith("insert into payments"));
        assertThat(handler.statements()).noneMatch(sql -> sql.startsWith("update orders"));
        assertThat(handler.ending()).isEqualTo("COMMIT");
        assertThat(database.orderLastChangedByTransaction(orderId)).isEqualTo(orderCreatedInTransaction);
        assertThat(database.paymentSavedByTransaction(orderId)).isEqualTo(handler.number());
        step("CONCLUSION: the status change never reached the database, and the database committed the rest of the handler's transaction.");
    }

    /** Hard evidence with the fix: the database received ROLLBACK instead of COMMIT, and nothing from the transaction remains. */
    @Test
    void databaseProof_withFix_theDatabaseRolledBackTheWholeTransaction() throws Exception {
        ConfigurableApplicationContext shop = startShop(true);
        paymentArrivesAndPodIsStoppedInTheMiddle(shop);

        HandlerTransaction handler = PostgresJournal.transactionThatSavedPaymentFor(orderId);
        step("Postgres log: everything it received from the handler in this transaction:" + handler.describe());
        step("Postgres about the order: it was last changed by transaction no. " + database.orderLastChangedByTransaction(orderId)
                + ", the one in which the test created the order (no. " + orderCreatedInTransaction + ")");
        step("Postgres about the payment: " + (database.paymentSavedByTransaction(orderId) < 0 ? "it is not in the database" : "it is there!"));

        assertThat(handler.ending()).isEqualTo("ROLLBACK");
        assertThat(database.orderLastChangedByTransaction(orderId)).isEqualTo(orderCreatedInTransaction);
        assertThat(database.paymentSavedByTransaction(orderId)).isEqualTo(-1);
        step("CONCLUSION: the database rolled back the handler's whole transaction. Nothing was saved halfway.");
    }

    /** The common start of every story: a payment arrives, and the pod is stopped while it is being handled. */
    private Outcome paymentArrivesAndPodIsStoppedInTheMiddle(ConfigurableApplicationContext shop) throws Exception {
        FraudCheckClient fraudCheck = shop.getBean(FraudCheckClient.class);
        ProcessingOutcomes outcomes = shop.getBean(ProcessingOutcomes.class);
        EntityManagerFactory hibernate = shop.getBean(EntityManagerFactory.class);

        orderCreatedInTransaction = database.insertNewOrder(orderId);
        step("The database has order " + orderId + ": " + database.describe(orderId));

        fraudCheck.respondSlowly();
        step("The customer pays: sending the payment message for " + orderId + " (the fraud check is slow today)");
        sendPaymentReceived(shop, orderId);
        fraudCheck.awaitRequest();

        step("Kubernetes stops the pod (SIGTERM) while the handler is in the middle of its transaction");
        Thread sigterm = new Thread(shop::close, "sigterm");
        sigterm.start();

        step("Spring waits for the handler: " + howLongSpringWaitsForTheHandler());
        waitUntilClosed(hibernate);
        step("Spring stopped waiting and closed Hibernate. The handler is still waiting for the fraud check");

        step("The fraud check finally answers, the handler finishes its transaction");
        fraudCheck.respondNow();
        Outcome outcome = outcomes.awaitNext();
        sigterm.join();
        step("The application has stopped");
        return outcome;
    }

    private ConfigurableApplicationContext startShop(boolean withFix) {
        step(withFix ? "Starting the shop application WITH THE FIX" : "Starting the shop application WITHOUT THE FIX");
        SpringApplicationBuilder app = new SpringApplicationBuilder(ShopApplication.class)
                .properties(brokerProperties())
                .properties(Map.of(
                        "shop.payments", paymentsQueue,
                        "spring.datasource.url", POSTGRES.getJdbcUrl(),
                        "spring.datasource.username", POSTGRES.getUsername(),
                        "spring.datasource.password", POSTGRES.getPassword()))
                .registerShutdownHook(false);
        if (withFix) {
            app.sources(PartialCommitGuard.class);
        }
        return app.run();
    }

    private void step(String text) {
        story.info("STEP {}: {}", ++step, text);
    }

    private static void waitUntilClosed(EntityManagerFactory hibernate) throws InterruptedException {
        for (int i = 0; i < 12_000 && hibernate.isOpen(); i++) {
            Thread.sleep(10);
        }
    }

    /** A look at the database from the outside, over a separate connection, the way a person would check it. */
    private static final class Database {

        private final JdbcTemplate jdbc = new JdbcTemplate(
                new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));

        /** Returns the number of the transaction in which the order was created. */
        long insertNewOrder(String orderId) {
            jdbc.update("insert into orders (id, status) values (?, 'NEW')", orderId);
            return orderLastChangedByTransaction(orderId);
        }

        /** xmin: in every row Postgres stores the number of the transaction that created the row's current version. */
        long orderLastChangedByTransaction(String orderId) {
            return jdbc.queryForObject("select xmin::text::bigint from orders where id = ?", Long.class, orderId);
        }

        long paymentSavedByTransaction(String orderId) {
            List<Long> xids = jdbc.queryForList("select xmin::text::bigint from payments where order_id = ?", Long.class, orderId);
            return xids.isEmpty() ? -1 : xids.get(0);
        }

        int paymentsFor(String orderId) {
            return jdbc.queryForObject("select count(*) from payments where order_id = ?", Integer.class, orderId);
        }

        String orderStatus(String orderId) {
            return jdbc.queryForObject("select status from orders where id = ?", String.class, orderId);
        }

        String describe(String orderId) {
            return "payments: " + paymentsFor(orderId) + ", order status: " + orderStatus(orderId);
        }
    }

    /** The statements Postgres received from the handler in one transaction, and how the transaction ended. */
    record HandlerTransaction(List<String> statements, long number, String ending) {

        String describe() {
            StringBuilder text = new StringBuilder();
            for (String sql : statements) {
                text.append("\n        ").append(sql).append(comment(sql));
            }
            return text.toString();
        }

        private static String comment(String sql) {
            if (sql.startsWith("insert into payments")) {
                return "   <- saves the payment";
            }
            if (sql.startsWith("select") && sql.contains("from orders")) {
                return "   <- reads the order";
            }
            if (sql.startsWith("update orders")) {
                return "   <- changes the order status";
            }
            if (sql.equals("COMMIT")) {
                return "   <- commit";
            }
            if (sql.equals("ROLLBACK")) {
                return "   <- rollback";
            }
            return "";
        }
    }

    /** Reads the Postgres log and extracts the handler's transaction from it. */
    private static final class PostgresJournal {

        static HandlerTransaction transactionThatSavedPaymentFor(String orderId) {
            List<String> lines = List.of(POSTGRES.getLogs().split("\\R"));
            Map<String, String> lastStatementOfConnection = new HashMap<>();
            String connection = null;
            int found = -1;
            for (int i = 0; i < lines.size() && found < 0; i++) {
                String line = lines.get(i);
                String pid = connectionOf(line);
                if (line.contains(" LOG:")) {
                    lastStatementOfConnection.put(pid, sqlOf(line));
                } else if (line.contains(" DETAIL:  parameters:") && line.contains("'" + orderId + "'")
                        && lastStatementOfConnection.getOrDefault(pid, "").startsWith("insert into payments")) {
                    connection = pid;
                    found = i;
                }
            }
            if (connection == null) {
                throw new IllegalStateException("the Postgres log has no payment insert for " + orderId);
            }
            int begin = found;
            while (begin > 0 && !(connection.equals(connectionOf(lines.get(begin))) && sqlOf(lines.get(begin)).equals("BEGIN"))) {
                begin--;
            }
            List<String> statements = new ArrayList<>();
            long number = -1;
            String ending = "none";
            for (int i = begin; i < lines.size(); i++) {
                String line = lines.get(i);
                if (!connection.equals(connectionOf(line)) || !line.contains(" LOG:")) {
                    continue;
                }
                String sql = sqlOf(line);
                statements.add(sql);
                if (i > found && (sql.equals("COMMIT") || sql.equals("ROLLBACK"))) {
                    number = Long.parseLong(line.replaceAll("^.* xid=(\\d+) .*$", "$1"));
                    ending = sql;
                    break;
                }
            }
            return new HandlerTransaction(statements, number, ending);
        }

        private static String connectionOf(String line) {
            int open = line.indexOf(" [");
            int close = line.indexOf("] ", open + 2);
            return open < 0 || close < 0 ? "" : line.substring(open + 2, close);
        }

        /** "LOG:  execute S_1: COMMIT" or "LOG:  statement: BEGIN" -> just the statement. */
        private static String sqlOf(String line) {
            String rest = line.substring(line.indexOf(" LOG:") + 5).trim();
            int colon = rest.indexOf(": ");
            return colon < 0 ? rest : rest.substring(colon + 2).trim();
        }
    }
}
