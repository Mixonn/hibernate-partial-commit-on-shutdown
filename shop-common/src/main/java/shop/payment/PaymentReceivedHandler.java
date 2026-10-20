package shop.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import shop.order.Order;
import shop.order.OrderRepository;

/** Handles the "customer paid for the order" message. Ordinary, everyday code. */
@Service
public class PaymentReceivedHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReceivedHandler.class);

    private final PaymentRepository payments;
    private final OrderRepository orders;
    private final FraudCheckClient fraudCheck;

    public PaymentReceivedHandler(PaymentRepository payments, OrderRepository orders, FraudCheckClient fraudCheck) {
        this.payments = payments;
        this.orders = orders;
        this.fraudCheck = fraudCheck;
    }

    @Transactional
    public void handle(String orderId) {
        payments.save(new Payment(orderId, 100));
        log.info("[handler] saved the payment for {} (the INSERT already went to the database, not committed yet)", orderId);

        Order order = orders.findById(orderId).orElseThrow();
        order.markPaid();
        log.info("[handler] marked order {} as PAID (for now only in Hibernate's memory)", orderId);

        log.info("[handler] calling the fraud check...");
        fraudCheck.verify(orderId);
        log.info("[handler] the fraud check answered, finishing the transaction (commit)");
    }
}
