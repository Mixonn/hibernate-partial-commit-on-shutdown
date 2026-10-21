package shop.messaging;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import shop.payment.PaymentService;

/** Receives the "customer paid for the order" message from RabbitMQ (payload: the order id). */
@Component
@Profile("rabbitmq")
class RabbitMqPaymentListener {

    private final PaymentService payments;

    RabbitMqPaymentListener(PaymentService payments) {
        this.payments = payments;
    }

    @RabbitListener(queues = "${shop.payments}")
    void onPaymentReceived(String orderId) {
        payments.recordPayment(orderId);
    }

    @Bean
    Queue paymentsQueue(@Value("${shop.payments}") String name) {
        return new Queue(name, false);
    }
}
