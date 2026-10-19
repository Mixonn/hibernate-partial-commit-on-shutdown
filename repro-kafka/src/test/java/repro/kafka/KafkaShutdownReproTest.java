package repro.kafka;

import java.util.Map;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import repro.KafkaReproApplication;
import repro.common.AbstractShutdownReproTest;

class KafkaShutdownReproTest extends AbstractShutdownReproTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

    static {
        KAFKA.start();
    }

    @Override
    protected Class<?> application() {
        return KafkaReproApplication.class;
    }

    @Override
    protected Map<String, Object> brokerProperties() {
        return Map.of("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void send(ConfigurableApplicationContext context, String msg) throws Exception {
        KafkaTemplate<Object, Object> template = context.getBean(KafkaTemplate.class);
        template.send(destination(), msg).get();
    }
}
