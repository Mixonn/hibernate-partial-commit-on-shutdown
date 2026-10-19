package emfveto;

import jakarta.transaction.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.EntityManagerHolder;
import org.springframework.transaction.TransactionExecution;
import org.springframework.transaction.TransactionExecutionListener;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Fix: przy starcie każdej nowej transakcji rejestruje w transakcji Hibernate {@link Synchronization}, która
 * tuż przed COMMIT, już po flushu, odmawia zatwierdzenia, jeśli fabryka sesji jest zamknięta. Wtedy Hibernate
 * robi rollback zamiast zatwierdzić tylko to, co trafiło do bazy przed pominiętym flushem.
 * <p>
 * Spring Boot podpina beany {@link TransactionExecutionListener} do automatycznie konfigurowanego transaction
 * managera. Tu weto obejmuje wszystkie transakcje JPA; w libce można je zawęzić do wątków konsumenta.
 */
@Configuration(proxyBeanMethods = false)
public class EmfCloseVetoConfiguration {

    @Bean
    EmfCloseVeto emfCloseVeto() {
        return new EmfCloseVeto();
    }

    public static final class CommitRefusedException extends IllegalStateException {

        CommitRefusedException() {
            super("EntityManagerFactory closed before commit: COMMIT refused, transaction rolled back");
        }
    }

    public static final class EmfCloseVeto implements TransactionExecutionListener {

        private static final Log log = LogFactory.getLog(EmfCloseVeto.class);

        @Override
        public void afterBegin(TransactionExecution transaction, Throwable beginFailure) {
            if (beginFailure != null || !transaction.isNewTransaction()) {
                return;
            }
            // Wyjątek stąd zostawiłby EntityManager, połączenie i synchronizację przypięte do wątku.
            try {
                for (Object resource : TransactionSynchronizationManager.getResourceMap().values()) {
                    if (resource instanceof EntityManagerHolder holder) {
                        Session session = holder.getEntityManager().unwrap(Session.class);
                        session.getTransaction().registerSynchronization(new CommitVeto(session));
                    }
                }
            } catch (RuntimeException e) {
                log.warn("EMF close veto not registered for this transaction", e);
            }
        }
    }

    static final class CommitVeto implements Synchronization {

        private final Session session;

        CommitVeto(Session session) {
            this.session = session;
        }

        /** Hibernate woła to po flushu i po całej kolejce akcji, a przed {@code Connection.commit()}. */
        @Override
        public void beforeCompletion() {
            if (session.getSessionFactory().isClosed() && !manualFlush()) {
                throw new CommitRefusedException();
            }
        }

        /** Puste i nigdy nie rzuca: wyjątek tutaj odebrałby afterCompletion kolejnym synchronizacjom. */
        @Override
        public void afterCompletion(int status) {
        }

        /** MANUAL (np. readOnly): managed flush i tak się nie wykonuje, więc wynik commitu jest taki jak zawsze. */
        private boolean manualFlush() {
            try {
                return session.getHibernateFlushMode() == FlushMode.MANUAL;
            } catch (RuntimeException e) {
                return false;
            }
        }
    }
}
