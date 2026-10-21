package shop.payment;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Records payments in a background thread (@Async), so whoever asked does not have to wait.
 */
@Service
public class BackgroundPayments {

    private final PaymentService payments;

    BackgroundPayments(PaymentService payments) {
        this.payments = payments;
    }

    @Async
    public void recordPayment(String orderId) {
        payments.recordPayment(orderId);
    }
}
