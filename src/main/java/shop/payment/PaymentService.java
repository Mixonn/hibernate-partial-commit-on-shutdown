package shop.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import shop.order.Order;
import shop.order.OrderRepository;

/** Records that the customer paid for the order. Ordinary, everyday code. */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final OrderRepository orders;
    private final FraudCheckClient fraudCheck;

    public PaymentService(PaymentRepository payments, OrderRepository orders, FraudCheckClient fraudCheck) {
        this.payments = payments;
        this.orders = orders;
        this.fraudCheck = fraudCheck;
    }

    @Transactional
    public void recordPayment(String orderId) {
        payments.save(new Payment(orderId, 100));
        log.info("[payments] saved the payment for {} (the INSERT already went to the database, not committed yet)", orderId);

        Order order = orders.findById(orderId).orElseThrow();
        order.markPaid();
        log.info("[payments] marked order {} as PAID (for now only in Hibernate's memory)", orderId);

        log.info("[payments] calling the fraud check...");
        fraudCheck.verify(orderId);
        log.info("[payments] the fraud check answered, finishing the transaction (commit)");
    }
}
