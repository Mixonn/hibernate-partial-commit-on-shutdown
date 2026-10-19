package repro.common;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** Zmiana wartości to UPDATE z dirty checkingu: czeka w Persistence Context na flush przy commicie. */
@Entity
@Table(name = "counter")
public class Counter {

    @Id
    private Long id;

    private int value;

    protected Counter() {
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
