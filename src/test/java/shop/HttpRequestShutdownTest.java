package shop;

import org.springframework.context.ConfigurableApplicationContext;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An employee records a payment in the admin panel: an ordinary HTTP request.
 */
class HttpRequestShutdownTest extends EntryPointShutdownStory {

    private final HttpClient browser = HttpClient.newHttpClient();
    private CompletableFuture<HttpResponse<String>> response;

    @Override
    String howThePaymentArrives(String orderId) {
        return "An employee records the payment in the admin panel: POST /admin/orders/" + orderId + "/payment";
    }

    @Override
    void paymentArrives(ConfigurableApplicationContext pod, String orderId) {
        HttpRequest request = HttpRequest.newBuilder(url(pod, "/admin/orders/" + orderId + "/payment"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        response = browser.sendAsync(request, BodyHandlers.ofString());
    }

    @Override
    String howLongSpringWaits() {
        return "for the HTTP request: 30 s (graceful shutdown of the web server)";
    }

    @Override
    String whatTheSenderSaw() throws Exception {
        try {
            return "HTTP " + response.get(30, SECONDS).statusCode();
        } catch (ExecutionException e) {
            return "no answer, the connection was closed (" + e.getCause().getClass().getSimpleName() + ")";
        }
    }
}
