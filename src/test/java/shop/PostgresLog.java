package shop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Reads the Postgres statement log and extracts from it the transaction in which the payment service saved a payment. */
final class PostgresLog {

    private PostgresLog() {
    }

    /** The statements Postgres received from the payment service in one transaction, and how the transaction ended. */
    record PaymentTransaction(List<String> statements, long number, String ending) {

        String describe() {
            StringBuilder text = new StringBuilder();
            for (String sql : statements) {
                text.append("\n        ").append(sql).append(comment(sql));
            }
            return text.toString();
        }

        private static String comment(String sql) {
            if (sql.startsWith("insert into payments")) {
                return "   <- saves the payment";
            }
            if (sql.startsWith("select") && sql.contains("from orders")) {
                return "   <- reads the order";
            }
            if (sql.startsWith("update orders")) {
                return "   <- changes the order status";
            }
            if (sql.equals("COMMIT")) {
                return "   <- commit";
            }
            if (sql.equals("ROLLBACK")) {
                return "   <- rollback";
            }
            return "";
        }
    }

    static PaymentTransaction transactionThatSavedPaymentFor(PostgreSQLContainer postgres, String orderId) {
        List<String> lines = List.of(postgres.getLogs().split("\\R"));
        Map<String, String> lastStatementOfConnection = new HashMap<>();
        String connection = null;
        int found = -1;
        for (int i = 0; i < lines.size() && found < 0; i++) {
            String line = lines.get(i);
            String pid = connectionOf(line);
            if (line.contains(" LOG:")) {
                lastStatementOfConnection.put(pid, sqlOf(line));
            } else if (line.contains(" DETAIL:  parameters:") && line.contains("'" + orderId + "'")
                    && lastStatementOfConnection.getOrDefault(pid, "").startsWith("insert into payments")) {
                connection = pid;
                found = i;
            }
        }
        if (connection == null) {
            throw new IllegalStateException("the Postgres log has no payment insert for " + orderId);
        }
        int begin = found;
        while (begin > 0 && !(connection.equals(connectionOf(lines.get(begin))) && sqlOf(lines.get(begin)).equals("BEGIN"))) {
            begin--;
        }
        List<String> statements = new ArrayList<>();
        long number = -1;
        String ending = "none";
        for (int i = begin; i < lines.size(); i++) {
            String line = lines.get(i);
            if (!connection.equals(connectionOf(line)) || !line.contains(" LOG:")) {
                continue;
            }
            String sql = sqlOf(line);
            statements.add(sql);
            if (i > found && (sql.equals("COMMIT") || sql.equals("ROLLBACK"))) {
                number = Long.parseLong(line.replaceAll("^.* xid=(\\d+) .*$", "$1"));
                ending = sql;
                break;
            }
        }
        return new PaymentTransaction(statements, number, ending);
    }

    private static String connectionOf(String line) {
        int open = line.indexOf(" [");
        int close = line.indexOf("] ", open + 2);
        return open < 0 || close < 0 ? "" : line.substring(open + 2, close);
    }

    /** "LOG:  execute S_1: COMMIT" or "LOG:  statement: BEGIN" -> just the statement. */
    private static String sqlOf(String line) {
        String rest = line.substring(line.indexOf(" LOG:") + 5).trim();
        int colon = rest.indexOf(": ");
        return colon < 0 ? rest : rest.substring(colon + 2).trim();
    }
}
