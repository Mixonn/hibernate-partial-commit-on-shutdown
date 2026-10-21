package shop.infra;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Turns on @Scheduled jobs and @Async methods.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableAsync
class BackgroundWork {
}
