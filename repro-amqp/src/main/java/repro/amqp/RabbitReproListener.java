package repro.amqp;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import repro.common.ListenerSupport;
import repro.common.ReproWork;

@Component
public class RabbitReproListener {

    private final ReproWork work;

    public RabbitReproListener(ReproWork work) {
        this.work = work;
    }

    @RabbitListener(queues = "${repro.destination}")
    public void on(String msg) {
        ListenerSupport.handle(work, msg);
    }

    @Bean
    Queue reproQueue(@Value("${repro.destination}") String queue) {
        return new Queue(queue, false);
    }
}
