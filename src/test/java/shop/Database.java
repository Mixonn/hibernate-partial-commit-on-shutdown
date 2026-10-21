package shop;

import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A look at the database from the outside, over a separate connection, the way a person would check it. */
class Database {

    private final JdbcTemplate jdbc;

    Database(PostgreSQLContainer postgres) {
        jdbc = new JdbcTemplate(new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));
    }

    /** Returns the number of the transaction in which the order was created. */
    long insertNewOrder(String orderId) {
        jdbc.update("insert into orders (id, status) values (?, 'NEW')", orderId);
        return orderLastChangedByTransaction(orderId);
    }

    /** xmin: in every row Postgres stores the number of the transaction that created the row's current version. */
    long orderLastChangedByTransaction(String orderId) {
        return jdbc.queryForObject("select xmin::text::bigint from orders where id = ?", Long.class, orderId);
    }

    long paymentSavedByTransaction(String orderId) {
        List<Long> xids = jdbc.queryForList("select xmin::text::bigint from payments where order_id = ?", Long.class, orderId);
        return xids.isEmpty() ? -1 : xids.get(0);
    }

    int paymentsFor(String orderId) {
        return jdbc.queryForObject("select count(*) from payments where order_id = ?", Integer.class, orderId);
    }

    String orderStatus(String orderId) {
        return jdbc.queryForObject("select status from orders where id = ?", String.class, orderId);
    }

    String describe(String orderId) {
        return "payments: " + paymentsFor(orderId) + ", order status: " + orderStatus(orderId);
    }
}
