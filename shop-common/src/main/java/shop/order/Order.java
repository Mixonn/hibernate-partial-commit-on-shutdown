package shop.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "orders")
public class Order {

    public enum Status { NEW, PAID }

    @Id
    private String id;

    @Enumerated(EnumType.STRING)
    private Status status = Status.NEW;

    protected Order() {
    }

    /** The change reaches the database only when Hibernate writes out its changes (flush), i.e. at the end of the transaction. */
    public void markPaid() {
        status = Status.PAID;
    }
}
