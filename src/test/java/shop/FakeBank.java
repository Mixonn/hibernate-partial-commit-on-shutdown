package shop;

import org.springframework.context.annotation.Primary;
import shop.bank.BankClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The test's stand-in for the bank's API: the test decides when a transfer arrives.
 */
@Primary
class FakeBank extends BankClient {

    private final Queue<String> newTransfers = new ConcurrentLinkedQueue<>();

    void transferArrives(String orderId) {
        newTransfers.add(orderId);
    }

    @Override
    public List<String> newIncomingTransfers() {
        List<String> transfers = new ArrayList<>();
        for (String orderId = newTransfers.poll(); orderId != null; orderId = newTransfers.poll()) {
            transfers.add(orderId);
        }
        return transfers;
    }
}
