package shop;

import java.util.List;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.rabbitmq.RabbitMQContainer;

class RabbitMqPaymentShutdownTest extends PaymentShutdownStory {

    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    static {
        RABBIT.start();
    }

    @Override
    List<String> brokerArguments() {
        return List.of(
                "--spring.profiles.active=rabbitmq",
                "--spring.rabbitmq.addresses=amqp://%s:%s@%s:%d".formatted(
                        RABBIT.getAdminUsername(), RABBIT.getAdminPassword(), RABBIT.getHost(), RABBIT.getAmqpPort()));
    }

    @Override
    void sendPaymentReceived(ConfigurableApplicationContext pod, String payments, String orderId) {
        pod.getBean(RabbitTemplate.class).convertAndSend(payments, orderId);
    }

    @Override
    String howLongSpringWaitsForThePaymentService() {
        return "with RabbitMQ 5 s (shutdownTimeout); then it interrupts the thread, but waiting for an HTTP response does not notice";
    }
}
