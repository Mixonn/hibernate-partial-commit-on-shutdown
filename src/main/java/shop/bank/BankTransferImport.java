package shop.bank;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import shop.payment.PaymentService;

/**
 * Customers can pay by bank transfer: every minute the shop asks the bank for new transfers and records each payment.
 */
@Component
class BankTransferImport {

    private final BankClient bank;
    private final PaymentService payments;

    BankTransferImport(BankClient bank, PaymentService payments) {
        this.bank = bank;
        this.payments = payments;
    }

    @Scheduled(fixedDelayString = "${shop.bank-transfers.check-every:PT1M}")
    void importNewTransfers() {
        for (String orderId : bank.newIncomingTransfers()) {
            payments.recordPayment(orderId);
        }
    }
}
