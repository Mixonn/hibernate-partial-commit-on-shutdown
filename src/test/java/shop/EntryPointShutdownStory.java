package shop;

import fix.PartialCommitGuard;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.postgresql.PostgreSQLContainer;
import shop.TransactionOutcomes.Outcome;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The same story without a broker: the payment comes in as an HTTP request, is picked up by a @Scheduled job, or is
 * recorded in an @Async task. Each of them runs on a thread that Spring waits for only for a while when the pod
 * stops, so each of them can be caught in the middle of its transaction.
 * <p>
 * Every pod is the real {@link ShopApplication}, as in {@link PaymentShutdownStory}. The test classes only say how
 * the payment arrives and what the one who sent it saw in the end.
 */
abstract class EntryPointShutdownStory {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withCommand("postgres", "-c", "fsync=off");

    static {
        POSTGRES.start();
    }

    private static final Logger story = LoggerFactory.getLogger("shop.STORY");

    private final String orderId = "order-" + UUID.randomUUID().toString().substring(0, 8);
    private final Database database = new Database(POSTGRES);
    private int step;

    /**
     * How the payment arrives, in words.
     */
    abstract String howThePaymentArrives(String orderId);

    /**
     * Makes the payment arrive at the pod.
     */
    abstract void paymentArrives(ConfigurableApplicationContext pod, String orderId) throws Exception;

    /**
     * What Spring waits for when the pod stops, and for how long, before it shuts down the rest of the application.
     */
    abstract String howLongSpringWaits();

    /**
     * What the one who sent the payment saw in the end.
     */
    abstract String whatTheSenderSaw() throws Exception;

    /**
     * Extra arguments for the pod, if this way of arriving needs any.
     */
    List<String> podArguments() {
        return List.of();
    }

    @Test
    void withoutFix_podShutdownSavesHalfOfThePayment() throws Exception {
        ConfigurableApplicationContext pod = startPod(false);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        step("According to the application: " + outcome);
        step("In the database: " + database.describe(orderId));
        step("The sender saw: " + whatTheSenderSaw());
        assertThat(outcome.committed()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(1);       // the payment is there...
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");   // ...but the order is still unpaid
        step("RESULT: payment saved, order unpaid. The data is inconsistent and there is no error in the logs.");
    }

    @Test
    void withFix_podShutdownSavesNothing() throws Exception {
        ConfigurableApplicationContext pod = startPod(true);

        Outcome outcome = paymentArrivesAndPodIsStoppedInTheMiddle(pod);

        step("According to the application: " + outcome);
        step("In the database: " + database.describe(orderId));
        step("The sender saw: " + whatTheSenderSaw());
        assertThat(outcome.rolledBack()).isTrue();
        assertThat(database.paymentsFor(orderId)).isEqualTo(0);
        assertThat(database.orderStatus(orderId)).isEqualTo("NEW");
        step("RESULT: nothing was saved halfway.");
    }

    /**
     * The common start of every story: a payment arrives, and the pod is stopped while it is being recorded.
     */
    private Outcome paymentArrivesAndPodIsStoppedInTheMiddle(ConfigurableApplicationContext pod) throws Exception {
        SlowFraudCheck fraudCheck = pod.getBean(SlowFraudCheck.class);
        TransactionOutcomes outcomes = pod.getBean(TransactionOutcomes.class);
        EntityManagerFactory hibernate = pod.getBean(EntityManagerFactory.class);

        database.insertNewOrder(orderId);
        step("The database has order " + orderId + ": " + database.describe(orderId));

        fraudCheck.respondSlowly();
        step(howThePaymentArrives(orderId) + " (the fraud check is slow today)");
        paymentArrives(pod, orderId);
        fraudCheck.awaitRequest();

        step("Kubernetes stops the pod (SIGTERM) while the payment service is in the middle of its transaction");
        Thread sigterm = new Thread(pod::close, "sigterm");
        sigterm.start();

        step("Spring waits " + howLongSpringWaits());
        waitUntilClosed(hibernate);
        step("Spring stopped waiting and closed Hibernate. The payment service is still waiting for the fraud check");

        step("The fraud check finally answers, the payment service finishes its transaction");
        fraudCheck.respondNow();
        Outcome outcome = outcomes.awaitNext();
        sigterm.join();
        step("The pod has stopped");
        return outcome;
    }

    /**
     * Starts a pod: the shop application as it is, configured through its arguments like a production pod.
     */
    private ConfigurableApplicationContext startPod(boolean withFix) {
        step(withFix ? "Starting a shop pod WITH THE FIX" : "Starting a shop pod WITHOUT THE FIX");
        List<String> args = new ArrayList<>(podArguments());
        args.add("--server.port=0");
        args.add("--spring.datasource.url=" + POSTGRES.getJdbcUrl());
        args.add("--spring.datasource.username=" + POSTGRES.getUsername());
        args.add("--spring.datasource.password=" + POSTGRES.getPassword());

        SpringApplication.Augmented shop = SpringApplication.from(ShopApplication::main)
                .with(SlowFraudCheck.class, FakeBank.class, TransactionOutcomes.class);
        if (withFix) {
            shop = shop.with(PartialCommitGuard.class);
        }
        return shop.run(args.toArray(String[]::new)).getApplicationContext();
    }

    /**
     * The address of something on the pod's web server.
     */
    static URI url(ConfigurableApplicationContext pod, String path) {
        return URI.create("http://localhost:" + pod.getEnvironment().getProperty("local.server.port") + path);
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
