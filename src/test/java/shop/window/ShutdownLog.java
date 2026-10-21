package shop.window;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Notes the exact moment (System.nanoTime()) of each line the pod logs while it shuts down. Logback calls it on the
 * logging thread, right when the line is logged, so the time is that of the event itself, not of the console write.
 * <p>
 * The lines that matter come from the application's own logging configuration: "Closing JPA EntityManagerFactory"
 * (Spring, right before it closes Hibernate) and "HikariPool-1 - Shutdown initiated..." (HikariCP). With
 * {@link #traceBeanDestruction()} it also sees every bean Spring destroys, in order.
 */
final class ShutdownLog extends AppenderBase<ILoggingEvent> {

    static final String CLOSING_HIBERNATE = "Closing JPA EntityManagerFactory";
    static final String POOL_SHUTDOWN = "Shutdown initiated";

    private static final Pattern DESTROYED_BEAN = Pattern.compile("^Invoking .*? on bean (?:with name )?'([^']+)'");

    record Entry(long nanos, String logger, String message, IThrowableProxy error) {
    }

    private final Queue<Entry> entries = new ConcurrentLinkedQueue<>();
    private volatile boolean closingHibernateLogged;

    /**
     * Attaches the log in front of the console, so that it notes each line before the line is written out.
     */
    static ShutdownLog attach() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        ShutdownLog log = new ShutdownLog();
        log.setContext(context);
        log.setName("shutdown-window");
        log.start();
        List<Appender<ILoggingEvent>> others = new ArrayList<>();
        root.iteratorForAppenders().forEachRemaining(others::add);
        others.forEach(root::detachAppender);
        root.addAppender(log);
        others.forEach(root::addAppender);
        return log;
    }

    void detach() {
        LoggerContext context = (LoggerContext) getContext();
        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).detachAppender(this);
        stop();
    }

    /**
     * Makes Spring log each bean it destroys (TRACE), so that the run shows what is closed between Hibernate and the
     * pool. It adds a few log lines to the shutdown, so the window is measured on runs without it.
     */
    void traceBeanDestruction() {
        LoggerContext context = (LoggerContext) getContext();
        context.getLogger("org.springframework.beans.factory.support.DisposableBeanAdapter").setLevel(Level.TRACE);
        context.getLogger("org.springframework.context.annotation.CommonAnnotationBeanPostProcessor").setLevel(Level.TRACE);
    }

    @Override
    protected void append(ILoggingEvent event) {
        long now = System.nanoTime();
        String message = event.getFormattedMessage();
        entries.add(new Entry(now, event.getLoggerName(), message, event.getThrowableProxy()));
        if (message != null && message.startsWith(CLOSING_HIBERNATE)) {
            closingHibernateLogged = true;
        }
    }

    boolean closingHibernateLogged() {
        return closingHibernateLogged;
    }

    /**
     * When the first line containing the text was logged, or {@link ShutdownStopwatch#NOT_SEEN}.
     */
    long whenLogged(String text) {
        return first(e -> e.message() != null && e.message().contains(text));
    }

    /**
     * The error the application got from the @Async task, as Spring logged it ("Unexpected exception occurred
     * invoking async method"), or null.
     */
    IThrowableProxy asyncTaskError() {
        for (Entry e : entries) {
            if (e.error() != null && e.logger().contains("AsyncUncaughtExceptionHandler")) {
                return e.error();
            }
        }
        return null;
    }

    /**
     * The beans Spring destroyed from Hibernate ({@code entityManagerFactory}) to the connection pool
     * ({@code dataSource}), both included, in that order, each with the time its destruction started
     * (System.nanoTime()). Needs {@link #traceBeanDestruction()}.
     */
    List<DestroyedBean> destructionFromHibernateToThePool() {
        List<DestroyedBean> destroyed = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Entry e : entries) {
            Matcher m = e.message() == null ? null : DESTROYED_BEAN.matcher(e.message());
            if (m != null && m.find() && seen.add(m.group(1))) {
                destroyed.add(new DestroyedBean(m.group(1), e.nanos()));
            }
        }
        int hibernate = indexOf(destroyed, "entityManagerFactory");
        int pool = indexOf(destroyed, "dataSource");
        return hibernate < 0 || pool < 0 ? List.of() : destroyed.subList(hibernate, pool + 1);
    }

    record DestroyedBean(String name, long nanos) {
    }

    private static int indexOf(List<DestroyedBean> beans, String name) {
        for (int i = 0; i < beans.size(); i++) {
            if (beans.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private long first(Predicate<Entry> which) {
        for (Entry e : entries) {
            if (which.test(e)) {
                return e.nanos();
            }
        }
        return ShutdownStopwatch.NOT_SEEN;
    }
}
