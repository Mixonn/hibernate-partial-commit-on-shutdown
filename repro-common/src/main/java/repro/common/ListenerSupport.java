package repro.common;

/** Wspólne obramowanie wywołania handlera w listenerach Kafki i RabbitMQ. */
public final class ListenerSupport {

    private ListenerSupport() {
    }

    public static void handle(ReproWork work, String msg) {
        int invocation = ++Hooks.invocations;
        try {
            work.process(msg);
        } catch (RuntimeException e) {
            if (invocation == 1) {
                Hooks.failure = e;
            }
            throw e;
        } finally {
            if (invocation == 1) {
                Hooks.finishedNanos = System.nanoTime();
                Hooks.finished.countDown();
            }
        }
    }
}
