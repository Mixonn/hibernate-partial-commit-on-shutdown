package shop.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import shop.payment.PaymentService;

/** Receives the "customer paid for the order" message from Kafka (payload: the order id). */
@Component
@Profile("kafka")
class KafkaPaymentListener {

    private final PaymentService payments;

    KafkaPaymentListener(PaymentService payments) {
        this.payments = payments;
    }

    @KafkaListener(topics = "${shop.payments}")
    void onPaymentReceived(String orderId) {
        payments.recordPayment(orderId);
    }

    @Bean
    NewTopic paymentsTopic(@Value("${shop.payments}") String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
