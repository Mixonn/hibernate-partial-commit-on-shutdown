package shop.kafka;

import java.util.Map;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import shop.PaymentShutdownStory;

class KafkaPaymentShutdownTest extends PaymentShutdownStory {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    static {
        KAFKA.start();
    }

    @Override
    protected Map<String, Object> brokerProperties() {
        return Map.of("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void sendPaymentReceived(ConfigurableApplicationContext shop, String orderId) throws Exception {
        shop.getBean(KafkaTemplate.class).send(paymentsQueue(), orderId).get();
    }

    @Override
    protected String howLongSpringWaitsForTheHandler() {
        return "with Kafka 30 s (the default time limit for each shutdown phase)";
    }
}
