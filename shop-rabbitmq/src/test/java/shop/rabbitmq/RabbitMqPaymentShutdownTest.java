package shop.rabbitmq;

import java.util.Map;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import shop.PaymentShutdownStory;

class RabbitMqPaymentShutdownTest extends PaymentShutdownStory {

    private static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    static {
        RABBIT.start();
    }

    @Override
    protected Map<String, Object> brokerProperties() {
        return Map.of(
                "spring.rabbitmq.host", RABBIT.getHost(),
                "spring.rabbitmq.port", RABBIT.getAmqpPort(),
                "spring.rabbitmq.username", RABBIT.getAdminUsername(),
                "spring.rabbitmq.password", RABBIT.getAdminPassword());
    }

    @Override
    protected void sendPaymentReceived(ConfigurableApplicationContext shop, String orderId) {
        shop.getBean(RabbitTemplate.class).convertAndSend(paymentsQueue(), orderId);
    }

    @Override
    protected String howLongSpringWaitsForTheHandler() {
        return "with RabbitMQ 5 s (shutdownTimeout); then it interrupts the thread, but waiting for an HTTP response does not notice";
    }
}
