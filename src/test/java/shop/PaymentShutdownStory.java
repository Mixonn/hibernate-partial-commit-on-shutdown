package shop;

import static org.assertj.core.api.Assertions.assertThat;

import fix.PartialCommitGuard;
import jakarta.persistence.EntityManagerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import shop.PostgresLog.PaymentTransaction;
import shop.TransactionOutcomes.Outcome;

/**
 * The story: a customer pays for an order, and while that payment is being recorded, Kubernetes stops the pod.
 * <p>
 * Every pod is the real {@link ShopApplication}, started through its {@code main} with the same kind of arguments a
 * production pod gets: the profile of the broker and the addresses of the broker and the database. The test adds
 * only a stand-in for the external fraud-check system and a recorder of how transactions ended.
 * <p>
 * The same story runs on Kafka and on RabbitMQ; the test classes only say which broker to start and how to send the
 * message. The steps of the story show up on the console as "STEP" lines.
 */
abstract class PaymentShutdownStory {

    /** Postgres writes every statement that actually reached it to its own log, together with the transaction number. */
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off", "-c", "log_statement=all", "-c", "log_line_prefix=%m [%p] xid=%x ");

    static {
        POSTGRES.start();
    }

    private static final Logger story = LoggerFactory.getLogger("shop.STORY");

    private final String orderId = "order-" + UUID.randomUUID().toString().substring(0, 8);
    private final String payments = "payments-" + UUID.randomUUID();
    private final Database database = new Database(POSTGRES);
    private long orderCreatedInTransaction;
    private int step;

    /** The arguments that connect a pod to the broker: its profile and its address. */
    abstract List<String> brokerArguments();

    /** Sends the "customer paid for the order" message to the given topic or queue. */
    abstract void sendPaymentReceived(ConfigurableApplicationContext pod, String payments, String orderId) throws Exception;

    /** How long Spring waits for the payment service before it gives up and shuts down the rest of the application. */
    abstract String howLongSpringWaitsForThePaymentService();

    @Test
    void withoutFix_podShutdownSavesHalfOfThePayment() throws Exception {
        ConfigurableApplicationContext pod = startPod(false);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        step("According to the application: " + outcome);
        step("In the database: " + database.describe(orderId));
        assertThat(outcome.committed()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(1);       // the payment is there...
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");   // ...but the order is still unpaid
        step("RESULT: payment saved, order unpaid. The data is inconsistent and there is no error in the logs.");
    }

    @Test
    void withFix_podShutdownSavesNothingAndTheRetrySucceeds() throws Exception {
        ConfigurableApplicationContext pod = startPod(true);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        step("According to the application: " + outcome);
        step("In the database: " + database.describe(orderId));
        assertThat(outcome.rolledBack()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(0);
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");

        step("The message was not acknowledged, so the broker still has it. A new pod starts");
        ConfigurableApplicationContext newPod = startPod(true);
        Outcome retry = newPod.getBean(TransactionOutcomes.class).awaitNext();
        step("The new pod processed the same message: " + retry);
        step("In the database: " + database.describe(orderId));
        newPod.close();
        assertThat(retry.committed()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(1);
        assertThat(database.orderStatus(orderId)).isEqualTo("PAID");
        step("RESULT: nothing was saved halfway, and after the restart the payment was processed in full.");
    }

    /**
     * Hard evidence without the fix, straight from Postgres: what the database received from the payment service and
     * which transaction last changed the order. It depends neither on the application's logs nor on the isolation level.
     */
    @Test
    void databaseProof_withoutFix_theStatusChangeNeverReachedTheDatabase() throws Exception {
        ConfigurableApplicationContext pod = startPod(false);
        paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        PaymentTransaction transaction = PostgresLog.transactionThatSavedPaymentFor(POSTGRES, orderId);
        step("Postgres log: everything it received from the payment service in this transaction:" + transaction.describe());
        step("Postgres about the order: it was last changed by transaction no. " + database.orderLastChangedByTransaction(orderId)
                + ", the one in which the test created the order (no. " + orderCreatedInTransaction + ")");
        step("Postgres about the payment: it was saved by transaction no. " + database.paymentSavedByTransaction(orderId)
                + ", the payment service's transaction (no. " + transaction.number() + ", ended with: " + transaction.ending() + ")");

        assertThat(transaction.statements()).anyMatch(sql -> sql.startsWith("insert into payments"));
        assertThat(transaction.statements()).noneMatch(sql -> sql.startsWith("update orders"));
        assertThat(transaction.ending()).isEqualTo("COMMIT");
        assertThat(database.orderLastChangedByTransaction(orderId)).isEqualTo(orderCreatedInTransaction);
        assertThat(database.paymentSavedByTransaction(orderId)).isEqualTo(transaction.number());
        step("CONCLUSION: the status change never reached the database, and the database committed the rest of the transaction.");
    }

    /** Hard evidence with the fix: the database received ROLLBACK instead of COMMIT, and nothing from the transaction remains. */
    @Test
    void databaseProof_withFix_theDatabaseRolledBackTheWholeTransaction() throws Exception {
        ConfigurableApplicationContext pod = startPod(true);
        paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        PaymentTransaction transaction = PostgresLog.transactionThatSavedPaymentFor(POSTGRES, orderId);
        step("Postgres log: everything it received from the payment service in this transaction:" + transaction.describe());
        step("Postgres about the order: it was last changed by transaction no. " + database.orderLastChangedByTransaction(orderId)
                + ", the one in which the test created the order (no. " + orderCreatedInTransaction + ")");
        step("Postgres about the payment: " + (database.paymentSavedByTransaction(orderId) < 0 ? "it is not in the database" : "it is there!"));

        assertThat(transaction.ending()).isEqualTo("ROLLBACK");
        assertThat(database.orderLastChangedByTransaction(orderId)).isEqualTo(orderCreatedInTransaction);
        assertThat(database.paymentSavedByTransaction(orderId)).isEqualTo(-1);
        step("CONCLUSION: the database rolled back the whole transaction. Nothing was saved halfway.");
    }

    /** The common start of every story: a payment arrives, and the pod is stopped while it is being recorded. */
    private Outcome paymentArrivesAndPodIsStoppedInTheMiddle(ConfigurableApplicationContext pod) throws Exception {
        SlowFraudCheck fraudCheck = pod.getBean(SlowFraudCheck.class);
        TransactionOutcomes outcomes = pod.getBean(TransactionOutcomes.class);
        EntityManagerFactory hibernate = pod.getBean(EntityManagerFactory.class);

        orderCreatedInTransaction = database.insertNewOrder(orderId);
        step("The database has order " + orderId + ": " + database.describe(orderId));

        fraudCheck.respondSlowly();
        step("The customer pays: sending the payment message for " + orderId + " (the fraud check is slow today)");
        sendPaymentReceived(pod, payments, orderId);
        fraudCheck.awaitRequest();

        step("Kubernetes stops the pod (SIGTERM) while the payment service is in the middle of its transaction");
        Thread sigterm = new Thread(pod::close, "sigterm");
        sigterm.start();

        step("Spring waits for the payment service: " + howLongSpringWaitsForThePaymentService());
        waitUntilClosed(hibernate);
        step("Spring stopped waiting and closed Hibernate. The payment service is still waiting for the fraud check");

        step("The fraud check finally answers, the payment service finishes its transaction");
        fraudCheck.respondNow();
        Outcome outcome = outcomes.awaitNext();
        sigterm.join();
        step("The pod has stopped");
        return outcome;
    }

    /** Starts a pod: the shop application as it is, configured through its arguments like a production pod. */
    private ConfigurableApplicationContext startPod(boolean withFix) {
        step(withFix ? "Starting a shop pod WITH THE FIX" : "Starting a shop pod WITHOUT THE FIX");
        List<String> args = new ArrayList<>(brokerArguments());
        args.add("--spring.datasource.url=" + POSTGRES.getJdbcUrl());
        args.add("--spring.datasource.username=" + POSTGRES.getUsername());
        args.add("--spring.datasource.password=" + POSTGRES.getPassword());
        args.add("--shop.payments=" + payments);

        SpringApplication.Augmented shop = SpringApplication.from(ShopApplication::main)
                .with(SlowFraudCheck.class, TransactionOutcomes.class);
        if (withFix) {
            shop = shop.with(PartialCommitGuard.class);
        }
        return shop.run(args.toArray(String[]::new)).getApplicationContext();
    }

    private void step(String text) {
        story.info("STEP {}: {}", ++step, text);
    }

    private static void waitUntilClosed(EntityManagerFactory hibernate) throws InterruptedException {
        for (int i = 0; i < 12_000 && hibernate.isOpen(); i++) {
            Thread.sleep(10);
        }
    }
}
