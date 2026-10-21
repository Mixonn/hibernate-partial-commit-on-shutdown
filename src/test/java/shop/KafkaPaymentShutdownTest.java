package shop;

import java.util.List;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

class KafkaPaymentShutdownTest extends PaymentShutdownStory {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    static {
        KAFKA.start();
    }

    @Override
    List<String> brokerArguments() {
        return List.of(
                "--spring.profiles.active=kafka",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers());
    }

    @Override
    @SuppressWarnings("unchecked")
    void sendPaymentReceived(ConfigurableApplicationContext pod, String payments, String orderId) throws Exception {
        pod.getBean(KafkaTemplate.class).send(payments, orderId).get();
    }

    @Override
    String howLongSpringWaitsForThePaymentService() {
        return "with Kafka 30 s (the default time limit for each shutdown phase)";
    }
}
