package repro.kafka;

import emfveto.EmfCloseVetoConfiguration;

/** Te same scenariusze co {@link KafkaShutdownReproTest}, z konfiguracją fixa dołożoną do kontekstu. */
class KafkaShutdownReproWithVetoTest extends KafkaShutdownReproTest {

    @Override
    protected Class<?>[] extraConfiguration() {
        return new Class<?>[] {EmfCloseVetoConfiguration.class};
    }
}
