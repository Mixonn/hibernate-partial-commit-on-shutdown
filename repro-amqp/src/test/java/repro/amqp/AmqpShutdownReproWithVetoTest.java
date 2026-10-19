package repro.amqp;

import emfveto.EmfCloseVetoConfiguration;

/** Te same scenariusze co {@link AmqpShutdownReproTest}, z konfiguracją fixa dołożoną do kontekstu. */
class AmqpShutdownReproWithVetoTest extends AmqpShutdownReproTest {

    @Override
    protected Class<?>[] extraConfiguration() {
        return new Class<?>[] {EmfCloseVetoConfiguration.class};
    }
}
