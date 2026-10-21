package fix;

import jakarta.transaction.Synchronization;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * The fix. Adding this configuration to the application is all it takes.
 * <p>
 * The problem: when Spring has closed Hibernate while a transaction is still running, Hibernate silently skips
 * writing the in-memory changes at commit, but still tells the database to commit. Only part of the data ends up
 * in the database.
 * <p>
 * The fix: at the start of every transaction we leave a "guard" in it. Hibernate asks the guard right before
 * committing, after it has written out the changes. If Hibernate is closed at that point, the guard refuses and
 * Hibernate rolls the whole transaction back. Either everything is saved, or nothing.
 */
@Configuration(proxyBeanMethods = false)
public class PartialCommitGuard {

    @Bean
    GuardInstaller partialCommitGuardInstaller() {
        return new GuardInstaller();
    }

    /** Spring Boot attaches this listener to the transaction manager on its own; it is called at the start of every transaction. */
    static final class GuardInstaller implements TransactionExecutionListener {

        private static final Logger log = LoggerFactory.getLogger(GuardInstaller.class);

        @Override
        public void afterBegin(TransactionExecution transaction, Throwable beginFailure) {
            if (beginFailure != null || !transaction.isNewTransaction()) {
                return;
            }
            try {
                for (Object resource : TransactionSynchronizationManager.getResourceMap().values()) {
                    if (resource instanceof EntityManagerHolder holder) {
                        Session session = holder.getEntityManager().unwrap(Session.class);
                        session.getTransaction().registerSynchronization(new Guard(session));
                    }
                }
            } catch (RuntimeException e) {
                // Never throw from here: it would break the start of the transaction.
                log.warn("Could not add the guard to the transaction", e);
            }
        }
    }

    /** The guard. Hibernate calls beforeCompletion() right before committing the transaction. */
    static final class Guard implements Synchronization {

        private final Session session;

        Guard(Session session) {
            this.session = session;
        }

        @Override
        public void beforeCompletion() {
            if (session.getSessionFactory().isClosed() && !readOnly()) {
                throw new HibernateClosedBeforeCommitException();
            }
        }

        @Override
        public void afterCompletion(int status) {
        }

        /** In read-only transactions Hibernate writes nothing anyway, so there is nothing to guard. */
        private boolean readOnly() {
            try {
                return session.getHibernateFlushMode() == FlushMode.MANUAL;
            } catch (RuntimeException e) {
                return false;
            }
        }
    }

    public static final class HibernateClosedBeforeCommitException extends IllegalStateException {

        HibernateClosedBeforeCommitException() {
            super("Hibernate was closed before the transaction was committed: rolling it back instead of saving only part of the data");
        }
    }
}
