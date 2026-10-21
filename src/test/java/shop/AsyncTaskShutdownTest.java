package shop;

import org.springframework.context.ConfigurableApplicationContext;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

/**
 * The payment provider's webhook: the shop answers 202 right away and records the payment in an @Async task.
 */
class AsyncTaskShutdownTest extends EntryPointShutdownStory {

    private final HttpClient paymentProvider = HttpClient.newHttpClient();
    private HttpResponse<String> response;

    @Override
    String howThePaymentArrives(String orderId) {
        return "The payment provider calls the webhook POST /webhooks/payment-provider for " + orderId
                + "; the shop answers right away and records the payment in an @Async task";
    }

    @Override
    void paymentArrives(ConfigurableApplicationContext pod, String orderId) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(url(pod, "/webhooks/payment-provider"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"orderId\": \"" + orderId + "\"}"))
                .build();
        response = paymentProvider.send(request, BodyHandlers.ofString());
    }

    @Override
    String howLongSpringWaits() {
        return "for the @Async task: 30 s (the default time limit for each shutdown phase)";
    }

    @Override
    String whatTheSenderSaw() {
        return "HTTP " + response.statusCode() + " right away: for the payment provider, the notification was delivered";
    }
}
