package shop;

import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

/**
 * The customer pays by bank transfer: a @Scheduled job asks the bank for new transfers and records the payment.
 */
class ScheduledJobShutdownTest extends EntryPointShutdownStory {

    /**
     * The job asks the bank every 0.1 s instead of every minute, so the test does not have to wait for it.
     */
    @Override
    List<String> podArguments() {
        return List.of("--shop.bank-transfers.check-every=PT0.1S");
    }

    @Override
    String howThePaymentArrives(String orderId) {
        return "The customer pays for " + orderId + " by bank transfer; the shop's @Scheduled job finds the transfer at the bank";
    }

    @Override
    void paymentArrives(ConfigurableApplicationContext pod, String orderId) {
        pod.getBean(FakeBank.class).transferArrives(orderId);
    }

    @Override
    String howLongSpringWaits() {
        return "for the @Scheduled job: 30 s (the default time limit for each shutdown phase)";
    }

    @Override
    String whatTheSenderSaw() {
        return "nothing: the bank only listed the transfer when the job asked for new ones";
    }
}
