package shop.bank;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The client of the bank's API. In this demo the bank never has new transfers.
 */
@Component
public class BankClient {

    /**
     * The ids of the orders for which the bank received a transfer since the last call.
     */
    public List<String> newIncomingTransfers() {
        // A real shop sends an HTTP request to the bank's API here.
        return List.of();
    }
}
