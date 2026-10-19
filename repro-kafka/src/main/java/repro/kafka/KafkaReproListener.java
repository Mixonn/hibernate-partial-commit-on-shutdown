package repro.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import repro.common.ListenerSupport;
import repro.common.ReproWork;

@Component
public class KafkaReproListener {

    private final ReproWork work;

    public KafkaReproListener(ReproWork work) {
        this.work = work;
    }

    @KafkaListener(topics = "${repro.destination}")
    public void on(String msg) {
        ListenerSupport.handle(work, msg);
    }

    @Bean
    NewTopic reproTopic(@Value("${repro.destination}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
