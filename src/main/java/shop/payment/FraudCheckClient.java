package shop.payment;

import org.springframework.stereotype.Component;

/** The client of the external fraud-check system. In this demo the system approves every payment right away. */
@Component
public class FraudCheckClient {

    public void verify(String orderId) {
        // A real shop sends an HTTP request to the fraud-check system here and waits for the answer.
    }
}
