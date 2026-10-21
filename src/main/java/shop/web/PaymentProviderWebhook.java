package shop.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import shop.payment.BackgroundPayments;

/**
 * The payment provider tells the shop that a customer paid. Providers expect a quick answer, so the shop answers
 * 202 Accepted right away and records the payment in the background.
 */
@RestController
class PaymentProviderWebhook {

    record PaymentNotification(String orderId) {
    }

    private final BackgroundPayments payments;

    PaymentProviderWebhook(BackgroundPayments payments) {
        this.payments = payments;
    }

    @PostMapping("/webhooks/payment-provider")
    ResponseEntity<Void> paymentReceived(@RequestBody PaymentNotification notification) {
        payments.recordPayment(notification.orderId());
        return ResponseEntity.accepted().build();
    }
}
