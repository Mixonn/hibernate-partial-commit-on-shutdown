package repro.common;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/** IDENTITY: Hibernate wysyła INSERT od razu przy persist, żeby poznać id. */
@Entity
@Table(name = "identity_row")
public class IdentityRow {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String msg;

    protected IdentityRow() {
    }

    public IdentityRow(String msg) {
        this.msg = msg;
    }
}
