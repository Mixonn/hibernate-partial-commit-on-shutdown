package shop.web;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import shop.payment.PaymentService;

/**
 * The admin panel: an employee records a payment the customer made by phone or at the counter.
 */
@RestController
class AdminPaymentController {

    private final PaymentService payments;

    AdminPaymentController(PaymentService payments) {
        this.payments = payments;
    }

    @PostMapping("/admin/orders/{orderId}/payment")
    void recordPayment(@PathVariable String orderId) {
        payments.recordPayment(orderId);
    }
}
