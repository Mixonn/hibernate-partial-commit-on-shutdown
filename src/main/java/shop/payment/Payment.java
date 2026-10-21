package shop.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "payments")
public class Payment {

    /**
     * The database generates the id (IDENTITY). Hibernate cannot keep this INSERT in memory until the end of the
     * transaction, like other changes: it sends it right away, on save(), to learn the id. That is why the payment
     * reaches the database even when the rest of the transaction does not.
     */
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
