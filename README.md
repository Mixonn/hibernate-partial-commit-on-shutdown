# emf-close-repro-spring

Reprodukcja częściowego commitu przy zamykaniu aplikacji, w prawdziwej aplikacji Spring Boot
z Kafką i z RabbitMQ, oraz test fixa. Bez żadnych zmian w Springu, Hibernate, HikariCP ani w kontenerach
listenerów.

Stos: **Spring Boot 4.1.1** (Spring 7.0.9, **Hibernate 7.4.5**, HikariCP), Postgres 16,
`apache/kafka-native:3.8.0`, `rabbitmq:4.1-management`, Testcontainers 2. To ta sama linia, której
używa firmowa libka w wersji na Boot 4. Wcześniej to samo przeszło na Spring Boot 3.5.16 / Hibernate 6.6 z identycznym
wynikiem (opis na dole).

## Moduły

| Moduł | Co zawiera |
|---|---|
| `repro-common` | encje, transakcja handlera (`ReproWork`), punkty kontrolne testu (`Hooks`), wspólny test (`AbstractShutdownReproTest`, jako test-jar) |
| `repro-veto` | **fix**: `EmfCloseVetoConfiguration`. Nie jest skanowany automatycznie, trzeba go dołożyć do kontekstu |
| `repro-kafka` | aplikacja z `@KafkaListener`; `KafkaShutdownReproTest` (bez fixa) i `KafkaShutdownReproWithVetoTest` (te same scenariusze z fixem) |
| `repro-amqp` | aplikacja z `@RabbitListener`; `AmqpShutdownReproTest` i `AmqpShutdownReproWithVetoTest` |

## Co robi handler

Jedna transakcja `@Transactional`:

1. `save(new IdentityRow(msg))`: encja z `IDENTITY`, więc INSERT idzie do bazy od razu;
2. `counter.setValue(value + 1)`: UPDATE czeka w Persistence Context na flush przy commicie;
3. praca bez żadnej operacji ORM na końcu transakcji (jak wywołanie HTTP).

Test wysyła wiadomość, czeka, aż handler będzie w kroku 3, i zamyka kontekst
(`ConfigurableApplicationContext.close()`, czyli to samo, co robi shutdown hook po SIGTERM).
Potem sprawdza w bazie: **FULL** (wiersz i licznik = 1), **PARTIAL** (wiersz bez licznika),
**NONE** (nic).

Jedyne, czym test steruje, to moment, w którym handler kończy krok 3.
Wariant „z wolnym beanem” dodaje bean, który przy zamykaniu pracuje 500 ms i niszczy się między EMF
a pulą Hikari. EMF zależy od niego tak jak od Flyway czy Liquibase. Odtwarza to większą aplikację,
w której w tym czasie zamyka się więcej rzeczy.

## Fix

`EmfCloseVetoConfiguration` rejestruje bean `TransactionExecutionListener`. Spring Boot sam podpina
go do transaction managera, a test to sprawdza. Przy starcie każdej nowej transakcji listener rejestruje
w transakcji Hibernate `jakarta.transaction.Synchronization`. Hibernate woła ją po flushu, a przed
`Connection.commit()`. Jeśli fabryka sesji jest zamknięta, synchronizacja rzuca wyjątek i Hibernate robi
rollback. Całość korzysta wyłącznie z publicznego API Springa i Hibernate.

Wyjątek, który dostaje aplikacja, to `JpaSystemException ← LocalSynchronizationException ←
CommitRefusedException`. Odmowę trzeba więc rozpoznawać po łańcuchu przyczyn (albo po fladze), a nie
po typie wyjątku na wierzchu.

## Wyniki: Spring Boot 4.1.1 / Hibernate 7.4.5 (19.10.2026)

| Scenariusz | Kafka bez fixa | Kafka z fixem | RabbitMQ bez fixa | RabbitMQ z fixem |
|---|---|---|---|---|
| kontrola: handler kończy się w 500 ms | FULL | FULL | FULL | FULL |
| handler w transakcji, gdy EMF się zamyka, minimalna aplikacja | **PARTIAL** z wyjątkiem I/O albo NONE: wyścig o milisekundy z Hikari, zdarzało się jedno i drugie | NONE (odmowa) | **PARTIAL**, wyjątek I/O | NONE (odmowa) |
| to samo z wolnym beanem (500 ms) | **PARTIAL, bez wyjątku** | NONE (odmowa) | **PARTIAL, bez wyjątku** | NONE (odmowa) |

Spring czeka na listener **30 s przy Kafce** (`spring.lifecycle.timeout-per-shutdown-phase`) i **5 s przy
RabbitMQ** (`shutdownTimeout`). Przy RabbitMQ wątek jest przerywany, ale handler czekający na gniazdo
tego nie zauważa.

Przegląd okna czasowego (wolny bean, handler kończy pracę w zadanym momencie od startu zamykania,
co 100 ms):

| Koniec pracy handlera | bez fixa (Kafka i RabbitMQ) | z fixem (Kafka i RabbitMQ) |
|---|---|---|
| przed zamknięciem EMF | FULL | FULL |
| od zamknięcia EMF do zamknięcia puli (ok. 500 ms) | **PARTIAL, bez wyjątku** | NONE (odmowa weta) |
| po zamknięciu puli Hikari | NONE, wyjątek „This connection has been closed” | NONE (odmowa weta) |

Z fixem w żadnym z przebiegów nie było częściowego zapisu.

Dowód od strony Postgresa (`log_statement=all` i `xmin`, test `evidence_serverSideViewOfPartialCommit`).

Bez fixa:

```
LOG:  statement: BEGIN
LOG:  execute <unnamed>: insert into identity_row (msg) values ($1)
LOG:  execute <unnamed>: select c1_0.id,c1_0.value from counter c1_0 where c1_0.id=$1
LOG:  execute S_1: COMMIT
```

Z fixem:

```
LOG:  statement: BEGIN
LOG:  execute <unnamed>: insert into identity_row (msg) values ($1)
LOG:  execute <unnamed>: select c1_0.id,c1_0.value from counter c1_0 where c1_0.id=$1
LOG:  execute S_1: ROLLBACK
```

UPDATE nigdy nie dociera do bazy, bo Hibernate pomija flush (`Skipping auto-flush due to session closed`).
Bez fixa `xmin` licznika zostaje taki jak przy wstawieniu na starcie testu, a nowy wiersz ma `xmin`
transakcji handlera, więc zatwierdzona została połowa transakcji. Izolacja nie ma tu znaczenia.
Z fixem nowego wiersza nie ma.

## Co z tego wynika

- Częściowy commit pojawia się wtedy, gdy handler jest w transakcji dłużej, niż Spring czeka na listenery,
  i kończy ją w oknie między zamknięciem EMF a zamknięciem puli połączeń.
- W minimalnej aplikacji okno ma pojedyncze milisekundy i zwykle kończy się wyjątkiem. Każda dodatkowa
  praca przy zamykaniu między EMF a pulą poszerza je, a wtedy częściowy commit przechodzi **bez żadnego
  wyjątku i bez logu powyżej TRACE**.
- Przy Kafce i domyślnym `terminationGracePeriodSeconds: 30` w Kubernetesie SIGKILL przychodzi mniej
  więcej wtedy, gdy kończy się 30-sekundowa faza. Przy dłuższym grace period ścieżka jest w pełni osiągalna.
- Fix zamienia każdy taki przypadek na rollback z rozpoznawalnym wyjątkiem i nie zmienia wyniku, gdy
  handler zdąży przed zamknięciem EMF.

## Spring Boot 3.5.16 / Hibernate 6.6 (pierwsza wersja repo)

Te same scenariusze bez fixa dały identyczne wyniki: PARTIAL z wyjątkiem I/O w minimalnej aplikacji,
PARTIAL bez wyjątku z wolnym beanem, to samo okno czasowe i ten sam log serwera. Repo zostało potem
przeniesione na Boot 4, więc tamte wyniki są tylko tutaj.

## Uruchomienie

Wymaga Dockera (np. OrbStack) i Javy 21+.

```bash
mvn -B install -DskipTests
mvn -B -pl repro-kafka test -Dtest='*ShutdownRepro*Test#control_*+minimalApp_*+appWithSlowShutdownBean_*+evidence_*'
mvn -B -pl repro-amqp test -Dtest='*ShutdownRepro*Test#control_*+minimalApp_*+appWithSlowShutdownBean_*+evidence_*'
mvn -B -pl repro-kafka test -Dtest='*ShutdownRepro*Test#sweep_*'   # ok. 13 min, oba warianty
```

Linie `RESULT`, `SWEEP` i `EVIDENCE` są w `repro-*/target/surefire-reports/*-output.txt`.
