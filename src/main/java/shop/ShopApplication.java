package shop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The shop application. It receives payment messages either from Kafka or from RabbitMQ, depending on the active
 * profile: {@code --spring.profiles.active=kafka} or {@code --spring.profiles.active=rabbitmq}.
 */
@SpringBootApplication
public class ShopApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShopApplication.class, args);
    }
}
