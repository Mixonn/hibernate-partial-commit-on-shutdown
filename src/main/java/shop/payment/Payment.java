package shop.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "payments")
public class Payment {

    /** The database assigns the id (IDENTITY), so Hibernate has to send the INSERT right away to learn it. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String orderId;

    private int amount;

    protected Payment() {
    }

    public Payment(String orderId, int amount) {
        this.orderId = orderId;
        this.amount = amount;
    }
}
