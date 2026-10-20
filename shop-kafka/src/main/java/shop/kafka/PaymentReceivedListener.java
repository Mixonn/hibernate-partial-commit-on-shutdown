package shop.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import shop.infra.ProcessingOutcomes;
import shop.payment.PaymentReceivedHandler;

/** Receives the "customer paid for the order" message from Kafka (payload: the order id). */
@Component
class PaymentReceivedListener {

    private final PaymentReceivedHandler handler;
    private final ProcessingOutcomes outcomes;

    PaymentReceivedListener(PaymentReceivedHandler handler, ProcessingOutcomes outcomes) {
        this.handler = handler;
        this.outcomes = outcomes;
    }

    @KafkaListener(topics = "${shop.payments}")
    void onPaymentReceived(String orderId) {
        outcomes.record(() -> handler.handle(orderId));
    }

    @Bean
    NewTopic paymentsTopic(@Value("${shop.payments}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
