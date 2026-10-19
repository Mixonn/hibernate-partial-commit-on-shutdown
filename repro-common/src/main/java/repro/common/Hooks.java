package repro.common;

import java.util.concurrent.CountDownLatch;

/**
 * Punkty kontrolne między testem a listenerem. Sterują tylko tym, co handler robi na końcu
 * swojej transakcji; Spring, Hibernate, Hikari i kontenery listenerów działają bez zmian.
 */
public final class Hooks {

    public enum Tail {
        /** Praca bez ORM przez stały czas. */
        SLEEP,
        /** Praca bez ORM, która kończy się zaraz po tym, jak EMF zostanie zamknięty. */
        UNTIL_EMF_CLOSED,
        /** Praca bez ORM, która kończy się w zadanym momencie liczonym od startu zamykania kontekstu. */
        UNTIL_OFFSET_FROM_CLOSE_START
    }

    public static volatile Tail tail = Tail.SLEEP;
    public static volatile long tailMillis;
    public static volatile long closeStartNanos;

    public static volatile CountDownLatch enteredTail = new CountDownLatch(1);
    public static volatile CountDownLatch finished = new CountDownLatch(1);
    public static volatile Throwable failure;
    public static volatile boolean interruptedDuringTail;
    public static volatile long emfClosedSeenNanos;
    public static volatile long tailExitNanos;
    public static volatile long finishedNanos;
    public static volatile int invocations;

    private Hooks() {
    }

    public static void reset(Tail newTail, long millis) {
        tail = newTail;
        tailMillis = millis;
        closeStartNanos = 0;
        enteredTail = new CountDownLatch(1);
        finished = new CountDownLatch(1);
        failure = null;
        interruptedDuringTail = false;
        emfClosedSeenNanos = 0;
        tailExitNanos = 0;
        finishedNanos = 0;
        invocations = 0;
    }
}
