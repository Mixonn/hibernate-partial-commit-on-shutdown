package repro.common;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jpa.autoconfigure.EntityManagerFactoryDependsOnPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Większa aplikacja niszczy między zamknięciem EMF a zamknięciem puli Hikari więcej beanów (executory czekające
 * na zadania, klienci innych systemów...). Ten bean odtwarza to w zwykły sposób: EMF zależy od niego (jak od
 * Flyway czy Liquibase), a on od DataSource, więc Spring niszczy go po EMF, a przed pulą połączeń.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "repro.slow-destroy-ms")
public class SlowShutdownConfig {

    @Bean
    SlowShutdownWork slowShutdownWork(DataSource dataSource, @Value("${repro.slow-destroy-ms}") long millis) {
        return new SlowShutdownWork(millis);
    }

    @Bean
    static EntityManagerFactoryDependsOnPostProcessor emfDependsOnSlowShutdownWork() {
        return new EntityManagerFactoryDependsOnPostProcessor("slowShutdownWork");
    }

    public static final class SlowShutdownWork {

        private final long millis;

        SlowShutdownWork(long millis) {
            this.millis = millis;
        }

        @PreDestroy
        void close() throws InterruptedException {
            Thread.sleep(millis);
        }
    }
}
