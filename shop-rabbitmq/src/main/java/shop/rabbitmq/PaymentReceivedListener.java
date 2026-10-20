package shop.rabbitmq;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import shop.infra.ProcessingOutcomes;
import shop.payment.PaymentReceivedHandler;

/** Receives the "customer paid for the order" message from RabbitMQ (payload: the order id). */
@Component
class PaymentReceivedListener {

    private final PaymentReceivedHandler handler;
    private final ProcessingOutcomes outcomes;

    PaymentReceivedListener(PaymentReceivedHandler handler, ProcessingOutcomes outcomes) {
        this.handler = handler;
        this.outcomes = outcomes;
    }

    @RabbitListener(queues = "${shop.payments}")
    void onPaymentReceived(String orderId) {
        outcomes.record(() -> handler.handle(orderId));
    }

    @Bean
    Queue paymentsQueue(@Value("${shop.payments}") String name) {
        return new Queue(name, false);
    }
}
