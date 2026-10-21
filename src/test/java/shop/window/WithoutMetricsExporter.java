package shop.window;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

/**
 * Takes the demo's {@code MetricsExporter} out of the shop. Without it nothing of the shop's own is closed between
 * Hibernate and the connection pool: what is left there is plain Spring Boot, Hibernate and HikariCP.
 * <p>
 * Spring calls this after it has found the shop's components, so the exporter and the bean that makes Hibernate start
 * after it are both already registered, and both are removed before anything is created.
 */
class WithoutMetricsExporter implements BeanDefinitionRegistryPostProcessor {

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) {
        registry.removeBeanDefinition("metricsExporter");
        registry.removeBeanDefinition("hibernateStartsAfterMetricsExporter");
    }
}
