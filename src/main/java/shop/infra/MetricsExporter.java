package shop.infra;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.jpa.autoconfigure.EntityManagerFactoryDependsOnPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * An example of an ordinary component that needs a moment when the application shuts down: it sends the metrics
 * it has not sent yet (0.5 s).
 * <p>
 * Spring closes it after Hibernate and before the database connection pool. Real applications usually have several
 * such components (executors, clients of other systems, caches), so a gap like this is normal.
 */
@Component("metricsExporter")
public class MetricsExporter {

    private static final Logger log = LoggerFactory.getLogger(MetricsExporter.class);

    /** It uses the database, so Spring closes it before the connection pool. */
    MetricsExporter(DataSource dataSource) {
    }

    @PreDestroy
    void flushRemainingMetrics() throws InterruptedException {
        log.info("[metrics] sending the remaining metrics before shutdown (0.5 s)");
        Thread.sleep(500);
    }

    /** Hibernate starts after the exporter, so Spring closes Hibernate before it. */
    @Bean
    static EntityManagerFactoryDependsOnPostProcessor hibernateStartsAfterMetricsExporter() {
        return new EntityManagerFactoryDependsOnPostProcessor("metricsExporter");
    }
}
