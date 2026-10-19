package repro.common;

import jakarta.persistence.EntityManagerFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReproWork {

    private final IdentityRowRepository rows;
    private final CounterRepository counters;
    private final EntityManagerFactory emf;

    public ReproWork(IdentityRowRepository rows, CounterRepository counters, EntityManagerFactory emf) {
        this.rows = rows;
        this.counters = counters;
        this.emf = emf;
    }

    /** Zwykła transakcja handlera: insert, który idzie od razu, update, który czeka na flush, potem praca bez ORM. */
    @Transactional
    public void process(String msg) {
        rows.save(new IdentityRow(msg));
        Counter counter = counters.findById(1L).orElseThrow();
        counter.setValue(counter.getValue() + 1);
        Hooks.enteredTail.countDown();
        tail();
    }

    /**
     * Końcówka transakcji bez żadnej operacji ORM, np. wywołanie HTTP. Przerwanie wątku nie przerywa
     * tej pracy (jak blokujący odczyt z gniazda), ale flaga przerwania zostaje ustawiona.
     */
    private void tail() {
        long start = System.nanoTime();
        boolean interrupted = false;
        while (!tailDone(start)) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            if (Thread.interrupted()) {
                interrupted = true;
            }
        }
        Hooks.tailExitNanos = System.nanoTime();
        if (interrupted) {
            Hooks.interruptedDuringTail = true;
            Thread.currentThread().interrupt();
        }
    }

    private boolean tailDone(long start) {
        long now = System.nanoTime();
        if (now - start > TimeUnit.MINUTES.toNanos(3)) {
            return true;
        }
        return switch (Hooks.tail) {
            case SLEEP -> now - start >= TimeUnit.MILLISECONDS.toNanos(Hooks.tailMillis);
            case UNTIL_EMF_CLOSED -> {
                if (emf.isOpen()) {
                    yield false;
                }
                Hooks.emfClosedSeenNanos = now;
                yield true;
            }
            case UNTIL_OFFSET_FROM_CLOSE_START -> Hooks.closeStartNanos != 0
                    && now - Hooks.closeStartNanos >= TimeUnit.MILLISECONDS.toNanos(Hooks.tailMillis);
        };
    }
}
