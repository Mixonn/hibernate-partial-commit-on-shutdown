package repro.amqp;

import java.util.Map;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import repro.AmqpReproApplication;
import repro.common.AbstractShutdownReproTest;

class AmqpShutdownReproTest extends AbstractShutdownReproTest {

    static final RabbitMQContainer RABBIT = new RabbitMQContainer("rabbitmq:4.1-management");

    static {
        RABBIT.start();
    }

    @Override
    protected Class<?> application() {
        return AmqpReproApplication.class;
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
    protected void send(ConfigurableApplicationContext context, String msg) {
        context.getBean(RabbitTemplate.class).convertAndSend(destination(), msg);
    }
}
