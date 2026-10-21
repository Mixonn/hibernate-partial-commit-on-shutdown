# How wide is the partial-commit window?

The demo widens the window on purpose: `MetricsExporter` takes 0.5 s to close, and Spring closes it after Hibernate
and before the connection pool. This note measures the window on the same shop **without** `MetricsExporter`, i.e. a
plain Spring Boot 4.1 + Hibernate 7.4 + HikariCP application, and with it for comparison.

**In short:** without `MetricsExporter` the window is **about 1.2 ms** (median 1.17 ms; 63 of 70 shutdowns at or under
1.5 ms, the longest 5.0 ms). A commit that starts in about its first half saves only the payment and reports success. A
commit that starts in the second half also saves only the payment, but the application gets an I/O error. Integrated
over the sweep, the window behaves like **0.5 ms of silent partial commits** within **1.3 ms of partial commits of
either kind**. None of the application's own beans is closed in it. With `MetricsExporter` the window is
501.6–512.6 ms, and every commit in it is silent.

## What exactly is measured

A late commit saves only part of the transaction when it falls between two moments:

1. **The window opens** when Hibernate's `SessionFactory` reaches `CLOSED`. From then on `SessionImpl.managedFlush()`
   finds the session no longer open and skips the flush (logged only at TRACE). `SessionFactoryImpl.close()` sets
   `CLOSED` in a `finally`, after closing its caches and query engine and before `serviceRegistry.destroy()`. While it
   is still `CLOSING`, a commit flushes normally.
2. **The window closes** when HikariCP closes the transaction's physical connection. `HikariDataSource.close()` logs
   "Shutdown initiated", and `HikariPool.shutdown()`, after stopping its own executors, aborts the connections still in
   use (`abortActiveConnections`, on a `connection-assassinator` thread). From then on the commit can only fail.

A commit whose flush check comes after 1 and whose `COMMIT` reaches Postgres before 2 saves the `INSERT` that
Hibernate had already sent and loses the status change. If the reply to that `COMMIT` also comes back before 2, the
application believes the transaction went through.

## The setup

| | |
|---|---|
| Application | the shop from this repo, unchanged, started through `ShopApplication.main` like a pod: Spring MVC on Tomcat 11.0.24, a `@Scheduled` job, `@Async` on Spring Boot's `applicationTaskExecutor`, Spring Data JPA, HikariCP with Spring Boot's defaults (pool of 10). No broker profile, except where a table says Kafka or RabbitMQ. |
| Variants | **without `MetricsExporter`**: a test-only `BeanDefinitionRegistryPostProcessor` ([`WithoutMetricsExporter`](../src/test/java/shop/window/WithoutMetricsExporter.java)) removes the `metricsExporter` bean, and the bean that makes Hibernate depend on it, before anything is created. **With `MetricsExporter`**: the demo as it is. |
| Versions | Spring Boot 4.1.1, Spring Framework 7.0.9, Spring Data JPA 4.1.1, Hibernate ORM 7.4.5.Final, HikariCP 7.0.2, PostgreSQL JDBC 42.7.13, Logback 1.5.38 |
| Database | Postgres 16.15 (`postgres:16-alpine`, `fsync=off`) in Docker 29.4.0 on OrbStack (linux/arm64 VM), reached over `localhost` |
| JVM | OpenJDK 25.0.1 with default flags (G1). The project compiles for Java 21. |
| Machine | Apple M4 Pro (14 cores), 24 GB, macOS 27.0.1. Other, idle containers were running alongside. |
| The transaction | `PaymentService.recordPayment` through the `@Async` `BackgroundPayments`: `INSERT` the payment (IDENTITY, so it is sent right away), mark the order `PAID` (only in Hibernate's memory), call the fraud check, commit. |
| The fraud check | [`TimedFraudCheck`](../src/test/java/shop/window/TimedFraudCheck.java) keeps the transaction open and answers a set time after Hibernate is closed, or as soon as "Closing JPA EntityManagerFactory" is logged. It spins on `SessionFactory.isOpen()` and `System.nanoTime()`, so it answers within a few microseconds of the chosen moment. |
| How long Spring waits | `spring.lifecycle.timeout-per-shutdown-phase=500ms` instead of the default 30 s. It only decides when Spring stops waiting for the `@Async` task; the shutdown after that is the same. |
| Warm-up | before the measured payment, 1000 payments through the same code, each of them really changing the order, so that the commit path (flush and `UPDATE` included) is warm, as in a pod that has served traffic. The shutdown code stays cold. |
| A fresh JVM per shutdown | [`ShutdownWindowProbe`](../src/test/java/shop/window/ShutdownWindowProbe.java) runs every shutdown in a JVM of its own, because a real pod shuts down once, with its shutdown code cold. The one-JVM table below shows why that matters. |
| Logging | as the application configures it (`application.properties`), to the console, redirected to a file. |

How the moments are taken: [`ShutdownStopwatch`](../src/test/java/shop/window/ShutdownStopwatch.java) spins on a thread
of its own over three flags that Hibernate, HikariCP and the Postgres driver keep anyway: `SessionFactory.isOpen()`,
`HikariDataSource.isClosed()` (set right before "Shutdown initiated" is logged) and `isClosed()` of the transaction's
physical `PgConnection` (set when HikariCP aborts or closes it). [`ShutdownLog`](../src/test/java/shop/window/ShutdownLog.java),
a Logback appender placed in front of the console, notes the moment each line is logged.
[`CommitClock`](../src/test/java/shop/window/CommitClock.java), a `TransactionExecutionListener`, notes when Spring starts
and finishes the commit and how it ended. The outcome is read from the database afterwards, over a separate
connection. The resolution is about 10 µs. Once in 160 runs the stopwatch thread was itself descheduled for about 1 ms
right when Hibernate closed (the fraud check, which watches the same flag, saw it 1 ms earlier); that run is the 0.000 ms
minimum and the 1.401 ms maximum in the first two rows below, and the 1.395 ms maximum in the demo's table is probably
the same thing.

## The window without `MetricsExporter`

160 shutdowns, each in a fresh JVM. Times in ms after Hibernate closed. When a commit came back before HikariCP got to
its connection, HikariCP closed that connection later as an idle one, so the window itself is taken from the 70 runs
in which the commit had not started yet when the connection was closed.

| Interval | Runs | Min | p10 | Median | p90 | Max |
|---|---|---|---|---|---|---|
| "Closing JPA EntityManagerFactory" logged → Hibernate closed (Hibernate's own close, before the window) | 160 | 0.284 | 0.303 | 0.328 | 0.410 | 1.401 |
| Hibernate closed → HikariCP starts shutting down | 160 | 0.000 | 0.444 | 0.481 | 0.577 | 1.031 |
| HikariCP starts shutting down → it closes the connection still in use | 70 | 0.584 | 0.612 | 0.684 | 0.863 | 4.446 |
| "Closing JPA EntityManagerFactory" logged → "HikariPool-1 - Shutdown initiated..." logged | 160 | 0.734 | 0.756 | 0.817 | 0.988 | 1.655 |
| Hibernate closed → HikariCP closes the transaction's connection, all runs | 160 | 0.991 | 1.051 | 1.171 | 1.402 | 5.028 |
| **Hibernate closed → HikariCP closes the connection still in use: the window** | **70** | **1.034** | **1.061** | **1.172** | **1.406** | **5.028** |

Where the 1.2 ms goes (medians): about **0.48 ms** from the closed flag until Spring calls `dataSource.close()`, which is
the rest of Hibernate's `close()` (the observers and `serviceRegistry.destroy()`) plus Spring moving to the next bean;
then about **0.68 ms** inside HikariCP, from "Shutdown initiated" until the connection-assassinator thread has aborted
the connection. The long tail (1.7, 2.0, 2.0 and 5.0 ms) is mostly HikariCP taking longer to get to the abort (up to
4.4 ms), once Hibernate's teardown (0.92 ms). Those runs logged nothing else in the meantime, and the traced runs below
show no other bean in that part of the shutdown.

### What is between Hibernate and the pool

Spring destroys beans in the reverse order of their creation. With bean destruction traced (TRACE for
`DisposableBeanAdapter` and `CommonAnnotationBeanPostProcessor`), the sequence from Hibernate to the pool, in µs after
Hibernate closed, is:

| Shop | Broker | Destroyed, in order |
|---|---|---|
| without `MetricsExporter` | none | `entityManagerFactory` −355 → `dataSource` +449 |
| without `MetricsExporter` | Kafka | `entityManagerFactory` −346 → `dataSource` +450 |
| without `MetricsExporter` | RabbitMQ | `entityManagerFactory` −365 → `dataSource` +462 |
| with `MetricsExporter` | none | `entityManagerFactory` −363 → `metricsExporter` +472 → `dataSource` +510 779 |

Nothing else. The Kafka template and producer factory, the RabbitMQ template and connection factory, the task scheduler
and the JPA mapping context are all destroyed before Hibernate. Spring Boot's `applicationTaskExecutor`, which runs the
`@Async` task, and the web filters are destroyed after the pool. With a broker the window was 1.02–1.34 ms with Kafka
(3 runs) and 1.11–2.91 ms with RabbitMQ (3 runs; the two longer ones again spent the extra time inside HikariCP).

## The sweep without `MetricsExporter`

10 shutdowns per row, each in a fresh JVM. "Commit starts" is Spring's `beforeCommit`, in ms after Hibernate closed
(median). A commit that is not cut off takes 0.7–1.0 ms from there to `afterCommit` on this setup.

| Fraud check answers | Commit starts | Full commit | Full, app got an error | **Partial, no error** | Partial, app got an error | Nothing saved, app got an error |
|---|---|---|---|---|---|---|
| when "Closing JPA EntityManagerFactory" is logged | −0.220 | 4 | 6 | | | |
| 0 ms after Hibernate is closed | 0.080 | | | **10** | | |
| 0.1 ms | 0.172 | | | **10** | | |
| 0.2 ms | 0.269 | | | **10** | | |
| 0.3 ms | 0.391 | | | **8** | 2 | |
| 0.4 ms | 0.491 | | | **8** | 2 | |
| 0.5 ms | 0.574 | | | **6** | 4 | |
| 0.7 ms | 0.774 | | | **1** | 9 | |
| 1 ms | 1.077 | | | | 9 | 1 |
| 1.5 ms | 1.579 | | | | 1 | 9 |
| 2 ms | 2.083 | | | | | 10 |
| 5 ms | 5.111 | | | | | 10 |
| 10 ms | 10.110 | | | | | 10 |
| 20 ms | 20.142 | | | | | 10 |
| 50 ms | 50.167 | | | | | 10 |
| 100 ms | 100.180 | | | | | 10 |

The errors the application got:

- with the data fully or partly saved: `DataAccessResourceFailureException ← PSQLException ← SocketException: Socket
  closed`. The `COMMIT` reached Postgres, and HikariCP closed the socket before the reply came back.
- with nothing saved: `DataAccessResourceFailureException ← PSQLException: This connection has been closed.` The
  connection was already closed when the commit started; Postgres rolled the transaction back when the socket closed.

Integrated over the sweep (the share of each outcome at each delay, times the spacing of the rows), the effective width
is **0.53 ms for silent partial commits** and **1.26 ms for partial commits of either kind**. Two earlier full runs
gave 0.51 and 1.13 ms, and 0.51 and 1.01 ms.

The first row is a control. A commit that starts while Hibernate is still closing does flush the status change, but
with the `UPDATE` to send as well it takes longer, and in 6 of 10 runs HikariCP closed the socket before the reply came
back: the data is complete, yet the application got an error.

## Many shutdowns in one JVM

The same shop, 40 shutdowns in a single JVM, the first one left out:

| Interval | Runs | Min | p10 | Median | p90 | Max |
|---|---|---|---|---|---|---|
| "Closing JPA EntityManagerFactory" logged → Hibernate closed | 39 | 0.054 | 0.061 | 0.081 | 0.141 | 0.176 |
| Hibernate closed → HikariCP starts shutting down | 39 | 0.064 | 0.067 | 0.082 | 0.123 | 0.199 |
| **the window** (connection still in use) | 19 | **0.275** | 0.289 | **0.394** | 0.515 | **0.516** |

With its shutdown code warmed up, the window shrinks to about 0.4 ms, shorter than a commit round trip here. Not one of
the 39 commits ended as a silent partial commit: those that started up to 0.5 ms after Hibernate closed mostly saved
only the payment and got "Socket closed" (20), the rest saved nothing (19). A pod shuts down once, cold, so the
fresh-JVM numbers above are the ones that apply.

## For comparison: the demo, with `MetricsExporter`

39 shutdowns, each in a fresh JVM, 3 per row.

| Interval | Runs | Min | p10 | Median | p90 | Max |
|---|---|---|---|---|---|---|
| "Closing JPA EntityManagerFactory" logged → Hibernate closed | 39 | 0.281 | 0.291 | 0.337 | 0.470 | 1.395 |
| Hibernate closed → HikariCP starts shutting down | 39 | 500.685 | 503.928 | 510.599 | 510.838 | 511.094 |
| **the window**, all runs | 39 | **501.570** | 505.124 | **511.369** | 512.308 | **512.639** |

The exporter's `Thread.sleep(500)` overslept by up to about 10 ms on this machine, which is where the spread comes from.

| Fraud check answers | Full commit | **Partial, no error** | Nothing saved, app got an error |
|---|---|---|---|
| when "Closing JPA EntityManagerFactory" is logged | 3 | | |
| 0, 1, 10, 100, 250, 450, 499, 500, 501 or 502 ms after Hibernate is closed | | **30** | |
| 505 ms | | **2** | 1 |
| 550 ms | | | 3 |

Every commit from the first microsecond of the window to its last millisecond saved the payment without the status
change, and the application saw a successful commit. The one failure at 505 ms is a run whose window had ended at
503.0 ms.

## What it means for real applications

- **A plain Spring Boot + Hibernate + HikariCP application has a window of about a millisecond, and none of the
  application's own code is in it.** It is Hibernate's own teardown after the closed flag is set (about 0.48 ms here)
  and HikariCP's shutdown until it aborts the connections still in use (about 0.68 ms, occasionally several ms). The
  usual infrastructure (web server, task executor and scheduler, Kafka and RabbitMQ clients) is destroyed before
  Hibernate or after the pool.
- **Inside that millisecond, a silent partial commit needs the whole commit round trip to fit.** Here a commit takes
  0.7–1.0 ms, so only commits that start in about the first half of the window end silently. Later ones still get their
  `COMMIT` to Postgres and save the payment without the status change, but the application gets "Socket closed". With a
  database across a real network, where a round trip alone can take as long as the whole window, the silent case all
  but disappears from a plain application. The "saved halfway, and the application got an error" case does not: it
  only needs the `COMMIT` to leave the client in time, not the reply to come back.
- **"The application got an error" is not a safe outcome.** The payment is in the database, the order is `NEW`, and
  the error invites a retry: the broker delivers the message again, an HTTP client tries again. Unless recording a
  payment is idempotent, the retry saves it twice. The README's "a later commit fails with an error instead of saving
  half of the data" holds once the connection is closed; in the last half-millisecond before that, the commit fails
  *and* saves half.
- **To hit the window at all, a transaction has to outlive Spring's patience** (30 s per shutdown phase by default,
  5 s for a RabbitMQ listener container) and then commit within that millisecond. If such a transaction commits at a
  random moment within the T seconds after Spring gives up, the chance of a partial commit is about 1.3 ms / T per such
  shutdown, and about 0.5 ms / T of a silent one: rare, but invisible when it happens.
- **The window is exactly as wide as whatever Spring destroys between the `EntityManagerFactory` and the
  `DataSource`.** Because Spring destroys in the reverse order of creation, what lands there is a bean that finished
  being created after the `DataSource` and before the `EntityManagerFactory`, and does real work when closed. The demo's
  `MetricsExporter` gets there on purpose, through an `EntityManagerFactoryDependsOnPostProcessor`; Spring Boot makes
  the `EntityManagerFactory` wait for Flyway's and Liquibase's initializers in a similar way, but those do their work at
  startup, not when closed. One such bean that takes 0.5 s turns the window into half a second in which every commit is
  a silent partial commit. In this shop, with or without a broker, there is no such bean. Whether an application has
  one is a matter of its creation order, not of how many components it has.
- **The first shutdown of a JVM is the one that counts.** Warmed up by many shutdowns in one JVM, the window shrinks to
  about 0.4 ms. A pod shuts down once, cold. On a pod with a CPU limit the cold code runs slower still, so the window
  there is likely wider than on this laptop; that was not measured.

## Running it

```bash
mvn -B test -Dtest=ShutdownWindowMeasurement
```

[`ShutdownWindowMeasurement`](../src/test/java/shop/window/ShutdownWindowMeasurement.java) is not part of the regular
test run: it starts about 210 JVMs and takes about 12 minutes. `-Dwindow.repeats=N` sets the shutdowns per row (default
10). It prints every table and writes them, with every single run (`*.tsv`) and the full console output of every JVM,
with millisecond timestamps (`logs/`), to `target/window-measurement/`. The numbers in this note come from the run of
21.10.2026; the tables here are those tables, with a few columns left out.
